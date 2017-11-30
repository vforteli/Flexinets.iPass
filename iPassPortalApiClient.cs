using FlexinetsDBEF;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.parser;
using log4net;
using LumenWorks.Framework.IO.Csv;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Handlers;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

namespace Flexinets.iPass
{
    public class iPassPortalApiClient
    {
        private readonly ILog _log = LogManager.GetLogger(typeof(iPassPortalApiClient));
        private readonly FlexinetsEntitiesFactory _contextFactory;
        private readonly String _username;
        private readonly String _password;


        public iPassPortalApiClient(FlexinetsEntitiesFactory contextFactory, String username, String password)
        {
            _contextFactory = contextFactory;
            _username = username;
            _password = password;
        }


        /// <summary>
        /// Download and bulk load the sessions
        /// </summary>
        /// <param name="fromdate"></param>
        /// <param name="todate"></param>
        [Obsolete]
        public async Task LoadSessionsAsync(DateTime fromdate, DateTime todate)
        {
            _log.Info($"Loading sessions from {fromdate} to {todate}");

            using (var db = _contextFactory.GetContext())
            {
                var customers = await GetAccountsAsync();
                using (var client = await CreateAuthenticatedHttpClientAsync())
                {
                    client.Timeout = TimeSpan.FromMinutes(2);
                    foreach (var customer in customers)
                    {
                        try
                        {
                            var url = $"https://openmobile.ipass.com/moservices/rest/api/ipass/{customer.customerId}/mo/reports/central-csv?report=connectionActivity&startdate={fromdate:MM-dd-yyyy}&enddate={todate:MM-dd-yyyy}&sess-type=initiated&pageSize=0";
                            using (var contentstream = await client.GetStreamAsync(url))
                            {
                                using (var connection = _contextFactory.GetSqlConnection())
                                {
                                    var reader = new StreamReader(contentstream);
                                    var content = await reader.ReadToEndAsync();
                                    content = content.Replace("\"", "");
                                    var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));
                                    var csvreader = new CsvReader(new StreamReader(stream), true, ',');
                                    var bulkcopy = new SqlBulkCopy(connection)
                                    {
                                        DestinationTableName = "Insight.SessionsV2",
                                        BulkCopyTimeout = 60,
                                        BatchSize = 10000
                                    };
                                    bulkcopy.ColumnMappings.Add(0, "AccountId");
                                    bulkcopy.ColumnMappings.Add(1, "[UsernameDomain]");
                                    bulkcopy.ColumnMappings.Add(3, "[ProfileId]");
                                    bulkcopy.ColumnMappings.Add(4, "[SessionId]");
                                    bulkcopy.ColumnMappings.Add(5, "[StartTime]");
                                    bulkcopy.ColumnMappings.Add(6, "[SessionLength]");
                                    bulkcopy.ColumnMappings.Add(7, "[AuthenticationTime]");
                                    bulkcopy.ColumnMappings.Add(8, "[ConnectionType]");
                                    bulkcopy.ColumnMappings.Add(9, "[ConnectionStatus]");
                                    bulkcopy.ColumnMappings.Add(10, "[ConnectionStatusCode]");
                                    bulkcopy.ColumnMappings.Add(11, "[ConnectionStatusCodeType]");
                                    bulkcopy.ColumnMappings.Add(12, "[ConnectionStatusCodeDescription]");
                                    bulkcopy.ColumnMappings.Add(13, "[DisconnectCode]");
                                    bulkcopy.ColumnMappings.Add(14, "[ClientIpAddress]");
                                    bulkcopy.ColumnMappings.Add(15, "[DownloadedMB]");
                                    bulkcopy.ColumnMappings.Add(17, "[UploadedMB]");
                                    bulkcopy.ColumnMappings.Add(18, "[SignalStrength]");
                                    bulkcopy.ColumnMappings.Add(19, "[Country]");
                                    bulkcopy.ColumnMappings.Add(20, "[ClientMacAddress]");
                                    bulkcopy.ColumnMappings.Add(21, "[Ssid]");
                                    bulkcopy.ColumnMappings.Add(22, "[AuthMethod]");
                                    bulkcopy.ColumnMappings.Add(23, "[SecurityMode]");
                                    bulkcopy.ColumnMappings.Add(24, "[DeviceType]");
                                    bulkcopy.ColumnMappings.Add(25, "[DeviceOS]");
                                    bulkcopy.ColumnMappings.Add(26, "[ClientVersion]");
                                    bulkcopy.ColumnMappings.Add(27, "[SdkVersion]");
                                    bulkcopy.ColumnMappings.Add(28, "[ConnectReason]");

                                    connection.Open();

                                    // Veeeewwy veeewwy important! Otherwise decimal parsing from eg 5.53 will fail
                                    Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
                                    await bulkcopy.WriteToServerAsync(csvreader);
                                    _log.Info($"Imported sessions for {customer.name}");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            _log.Error($"{customer.name} failed to download: ", ex);
                        }
                    }
                }

                _log.Info("Updating nodeids");
                var update = @"
BEGIN TRANSACTION
UPDATE Insight.SessionsV2
SET node_id = users.node_id
FROM Insight.SessionsV2 AS i LEFT JOIN users ON CONCAT(users.username, '@', users.realm) = i.UsernameDomain
WHERE CONCAT(users.username, '@', users.realm) = i.UsernameDomain AND i.node_id IS NULL

UPDATE Insight.SessionsV2
SET node_id = (SELECT TOP 1 t1.node_id
                FROM ipass_realms AS t1
                LEFT JOIN directory ON(t1.node_id = directory.node_id)
                WHERE realm = SUBSTRING(UsernameDomain, LEN(usernamedomain) - CHARINDEX('@', REVERSE(UsernameDomain)) + 2, LEN(UsernameDomain))
                ORDER BY directory.lft)
FROM Insight.SessionsV2 AS i
WHERE i.node_id IS NULL

UPDATE Insight.SessionsV2
SET node_id = (SELECT TOP 1 node_id
                FROM directory
                WHERE ipassCustomerID = AccountId
                ORDER BY directory.lft)
FROM Insight.SessionsV2 AS i
WHERE i.node_id IS NULL

COMMIT";
                await db.Database.ExecuteSqlCommandAsync(update);


                _log.Info("Updating summary tables");
                var updatesummary = @"
DELETE FROM ipass.CombinedCdrUsers WHERE Month >= @fromdate
INSERT INTO ipass.CombinedCdrUsers (Month, UsernameDomain, NodeId, GiSessions, DsSessions, Sum, Seconds)

SELECT 
	monthdatetime,
	UsernameDomain,
	node_id,
	SUM(CASE WHEN c.type = 'gi' THEN 1 ELSE 0 END) AS gisessions,
	SUM(CASE WHEN c.type = 'ds' THEN 1 ELSE 0 END) AS dssessions,
	SUM(sum) AS sum,
	SUM(duration) AS seconds

FROM ipass.CdrCombined AS c
WHERE node_id IS NOT NULL AND UsernameDomain IS NOT NULL AND monthdatetime >= @fromdate
GROUP BY monthdatetime, UsernameDomain,node_id

DELETE FROM ipass.combinedcdrgroups WHERE month >= @fromdate
INSERT INTO iPass.CombinedCdrGroups (Month, NodeId, Seconds, Sum, BothUsers, GiUsers, DsUsers, DsSessions, GiSession)

SELECT 
	u.Month,
	u.NodeId,
	SUM(u.Seconds),
	SUM(u.Sum),
	SUM(CASE WHEN u.DsSessions > 0 AND u.GiSessions > 0 THEN 1 ELSE 0 END) AS bothuser,
	SUM(CASE WHEN u.DsSessions = 0 AND u.GiSessions > 0 THEN 1 ELSE 0 END) AS giuser,
	SUM(CASE WHEN u.DsSessions > 0 AND u.GiSessions = 0 THEN 1 ELSE 0 END) AS dsuser,
	SUM(u.DsSessions),
	SUM(u.GiSessions)
	
FROM iPass.CombinedCdrUsers AS u
WHERE Month >= @fromdate
GROUP BY u.Month, u.NodeId";

                var summaryfromdate = new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.AddMonths(-1).Month, 1);
                await db.Database.ExecuteSqlCommandAsync(updatesummary, new SqlParameter("@fromdate", summaryfromdate));
            }
            _log.Info("Import done");
        }


        /// <summary>
        /// Download and bulk load the sessions
        /// </summary>
        /// <param name="customerId"></param>
        /// <param name="fromdate"></param>
        /// <param name="todate"></param>        
        public async Task<MemoryStream> LoadSessionsAsync(Int32 customerId, DateTime fromdate, DateTime todate)
        {
            using (var client = await CreateAuthenticatedHttpClientAsync())
            {
                client.Timeout = TimeSpan.FromMinutes(2);

                var url = $"https://openmobile.ipass.com/moservices/rest/api/ipass/{customerId}/mo/reports/central-csv?report=connectionActivity&startdate={fromdate:MM-dd-yyyy}&enddate={todate:MM-dd-yyyy}&sess-type=initiated&pageSize=0";
                using (var contentstream = await client.GetStreamAsync(url))
                {
                    using (var connection = _contextFactory.GetSqlConnection())
                    {
                        var reader = new StreamReader(contentstream);
                        var content = await reader.ReadToEndAsync();
                        content = content.Replace("\"", "");
                        var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));
                        return stream;
                    }
                }
            }
        }


        /// <summary>
        /// Get a list of active devices
        /// </summary>
        /// <param name="fromdate"></param>
        /// <param name="todate"></param>
        /// <returns></returns>
        public async Task GetActiveDevices(DateTime fromdate, DateTime todate)
        {
            _log.Info($"Loading active devices from {fromdate} to {todate}");

            var total = 0;
            var customers = await GetAccountsAsync();
            using (var client = await CreateAuthenticatedHttpClientAsync())
            {
                client.Timeout = TimeSpan.FromMinutes(2);
                foreach (var customer in customers)
                {
                    try
                    {
                        var url = $"https://openmobile.ipass.com/moservices/rest/api/ipass/{customer.customerId}/mo/reports/central?report=deviceActivitySummary&report=deviceActivityReport&startdate={fromdate:MM-dd-yyyy}&enddate={todate:MM-dd-yyyy}&pageSize=0";
                        var document = XDocument.Parse(await client.GetStringAsync(url));
                        var count = document.Descendants("r").Count();
                        total += count;
                        _log.Debug($"{customer.name}:{count}");

                    }
                    catch (Exception ex)
                    {
                        _log.Error($"{customer.name} failed to download: ", ex);
                    }
                }
                _log.Debug(total);
            }

            _log.Info("Import done");
        }


        /// <summary>
        /// Gets a list of the currently active users visible to ipass ~installed clients
        /// </summary>
        /// <returns></returns>
        public async Task GetActiveDeploymentAsync()
        {
            _log.Info("Getting active deployment");

            var sb = new StringBuilder();
            var list = new List<iPassDeviceModel>();

            using (var client = await CreateAuthenticatedHttpClientAsync())
            {
                var customers = await GetAccountsAsync();
                foreach (var customer in customers)
                {
                    try
                    {
                        var url = $"https://openmobile.ipass.com/moservices/rest/api/ipass/{customer.customerId}/mo/reports/central-csv?report=activeDeployment&pageSize=0";
                        using (var contentstream = await client.GetStreamAsync(url))
                        {
                            var sr = new StreamReader(contentstream);

                            //while (!sr.EndOfStream)
                            //{
                            //    var line = await sr.ReadLineAsync();
                            //    var parts = line.Split(new char[] { ',' });
                            //    list.Add(new iPassDevice
                            //    {
                            //        CustomerId = customer.AccountId,
                            //        UserId = parts[0],
                            //        ClientId = parts[1],
                            //        SoftwareVersion = parts[2],
                            //        Platform = parts[3],
                            //        OperatingSystem = parts[4],
                            //        ProfileIdVersion = parts[5],
                            //        LastAttempt = parts[6],
                            //        LastSuccessfullyInitiated = parts[7]
                            //    });
                            //}

                            sb.Append(await sr.ReadToEndAsync());
                            _log.Info($"Imported devices for {customer.name}");
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.Error(customer.name + " failed to download: ", ex);
                    }
                }

                using (var foo = new StreamWriter("c:/users/verne_000/desktop/derp.csv"))
                {
                    await foo.WriteLineAsync(sb.ToString());
                }
            }
        }


        /// <summary>
        /// Create an authenticated HttpClient
        /// </summary>
        /// <returns></returns>
        private async Task<HttpClient> CreateAuthenticatedHttpClientAsync(ProgressMessageHandler handler = null)
        {
            var data = new FormUrlEncodedContent(new List<KeyValuePair<String, String>>
            {
                new KeyValuePair<String, String>("username", _username),
                new KeyValuePair<String, String>("password", _password),
            });

            var client = handler != null ? new HttpClient(handler) : new HttpClient();
            await client.PostAsync("https://openmobile.ipass.com/moservices/rest/api/login", data);
            return client;
        }


        /// <summary>
        /// Update the account list from ipass
        /// </summary>
        /// <returns></returns>
        [Obsolete]
        private async Task UpdateAccountsAsync()
        {
            using (var db = _contextFactory.GetContext())
            {
                var accounts = await db.Accounts.ToListAsync();

                foreach (var account in await GetAccountsAsync())
                {
                    if (!(accounts.Any(o => o.AccountId == account.customerId)))
                    {
                        _log.Debug($"Found new account: {account.name}, {account.customerId}");
                        db.Accounts.Add(new Account
                        {
                            AccountId = account.customerId,
                            Name = account.name
                        });
                    }
                }

                await db.SaveChangesAsync();
            }
        }


        /// <summary>
        /// Get a list of customers from ipass
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<(Int32 customerId, String name)>> GetAccountsAsync()
        {
            var list = new List<(Int32 customerId, String name)>();

            var document = new XmlDocument();
            using (var client = await CreateAuthenticatedHttpClientAsync())
            {
                var response = await client.GetStringAsync("https://openmobile.ipass.com/moservices/rest/api/ipass/1029590/mo/UPAC/Company/1029590?childDepth=all");
                document.LoadXml(response);
                var nodes = document.GetElementsByTagName("child");
                foreach (XmlNode node in nodes)
                {
                    var accountId = Convert.ToInt32(node["id"].InnerText);
                    list.Add((Convert.ToInt32(node["id"].InnerText), node["name"].InnerText));
                }
                list.Add((1029590, "Flexinets root"));
                return list;
            }
        }


        /// <summary>
        /// Refresh profile list
        /// </summary>
        /// <returns></returns>
        public async Task RefreshProfilesAsync()
        {
            _log.Info("Refreshing profiles async");

            using (var db = _contextFactory.GetContext())
            {
                _log.Info("Removing old profiles");
                await db.Database.ExecuteSqlCommandAsync("TRUNCATE TABLE Insight.Profiles");

                foreach (var profile in await GetProfilesAsync())
                {
                    _log.Debug("Adding profile " + profile.Name);
                    try
                    {
                        db.Profiles.Add(new Profile
                        {
                            ProfileId = profile.ProfileId,
                            AccountId = profile.AccountId,
                            Name = profile.Name,
                            ProfileVersion = profile.ProfileVersion,
                            Status = profile.Status,
                            Platform = profile.Platform,
                            SoftwareVersion = profile.SoftwareVersion,
                            DateModified = profile.DateModified,
                            Pin = profile.Pin
                        });
                    }
                    catch (Exception ex)
                    {
                        _log.Error($"Failed adding profile {profile.Name}", ex);
                    }
                }

                await db.SaveChangesAsync();

                _log.Info("Profile refresh done...");
            }
        }


        /// <summary>
        /// Get a list of all ipass profiles
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<ProfileModel>> GetProfilesAsync()
        {
            using (var client = await CreateAuthenticatedHttpClientAsync())
            {
                var accounts = await GetAccountsAsync();

                var nodelist = new List<XmlNode>();
                Task.WaitAll(accounts.Select(customer => Task.Factory.StartNew(() =>
                {
                    _log.Info("Getting profiles for customerId " + customer.name);

                    var url = $"https://openmobile.ipass.com/moservices/rest/api/ipass/{customer.customerId}/mo/profileservice/profile?action=getpaginatedlist&includechildren=false&resultsthispage=200&pagenumber=1";
                    var document = new XmlDocument();
                    document.LoadXml(client.GetStringAsync(url).Result);

                    var nodes = document.GetElementsByTagName("Profile");
                    foreach (XmlNode node in nodes)
                    {
                        nodelist.Add(node);
                    }
                })).ToArray());

                var list = new List<ProfileModel>();
                foreach (var node in nodelist)
                {
                    list.Add(new ProfileModel
                    {
                        ProfileId = Convert.ToInt32(node.Attributes["id"].Value),
                        AccountId = Convert.ToInt32(node["AccountID"].InnerText),
                        Name = node["Name"].InnerText,
                        ProfileVersion = node["Version"].InnerText,
                        Status = node["Status"].InnerText,
                        Platform = node["TargetBundle"]["Platform"].InnerText,
                        SoftwareVersion = node["TargetBundle"]["Version"].InnerText,
                        DateModified = new DateTime(1970, 1, 1, 0, 0, 0, 0).AddSeconds(Convert.ToDouble(Convert.ToDecimal(node["LastModifiedDate"].InnerText)) / 1000),
                        Pin = node["Pin"].InnerText
                    });
                }

                return list;
            }
        }


        /// <summary>
        /// Downloads a profile from ipass
        /// </summary>
        /// <param name="profileId"></param>
        /// <param name="profileVersion"></param>
        /// <returns></returns>        
        public async Task<iPassClientFile> DownloadProfileAsync(Int32 profileId, String profileVersion, ProgressMessageHandler handler = null)
        {
            var url = $"https://openmobile.ipass.com/moservices/rest/api/ipass/1029590/mo/configurationservice/configuration?id={profileId}.V{profileVersion}&action=getbundle";
            var client = await CreateAuthenticatedHttpClientAsync(handler);
            var response = await client.GetAsync(url);
            return new iPassClientFile
            {
                Filename = WebUtility.UrlDecode(response.Content.Headers.ContentDisposition.FileName),
                ContentLength = response.Content.Headers.ContentLength.Value,
                ContentStream = await response.Content.ReadAsStreamAsync()
            };
        }


        /// <summary>
        /// Get a list of all roamservers for all customers
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<RoamserverModel>> GetRoamserversAsync()
        {
            /*
            <List>
              <serverInfo>
                <companyId>1043091</companyId>
                <connectionName>193.182.61.230</connectionName>
                <id>66126</id>
                <ipAddress>193.182.61.230</ipAddress>
                <port>577</port>
                <priority>1</priority>
                <serverType>ROAM_SERV</serverType>
                <acctIdleTimeout>30000</acctIdleTimeout>
                <connSharing>0</connSharing>
                <idleTimeout>120000</idleTimeout>
                <modifiedBy>1</modifiedBy>
                <modifiedDate>2013-09-28T02:17:43Z</modifiedDate>
                <numRetry>0</numRetry>
                <retryRsDelay>15</retryRsDelay>
                <email>philharmonic@trial.ipass.com</email>
                <firstName>iPass</firstName>
                <lastName>Admin</lastName>
              </serverInfo>
            </List>
             */

            var list = new List<XmlDocument>();
            var roamservers = new List<RoamserverModel>();

            using (var client = await CreateAuthenticatedHttpClientAsync())
            {
                var customers = await GetAccountsAsync();

                foreach (var customer in customers)
                {
                    var url = $"https://openmobile.ipass.com/moservices/rest/api/ipass/{customer.customerId}/mo/servermgmt/provisioning?action=roamservers";
                    var document = new XmlDocument();
                    document.Load(await client.GetStreamAsync(url));

                    foreach (XmlNode node in document.GetElementsByTagName("serverInfo"))
                    {
                        roamservers.Add(new RoamserverModel
                        {
                            CompanyId = node["companyId"].InnerText,
                            IpAddress = node["ipAddress"].InnerText,
                            Port = Convert.ToInt32(node["port"].InnerText),
                            Priority = Convert.ToInt32(node["priority"].InnerText),
                            DateModified = DateTime.Parse(node["modifiedDate"].InnerText),
                            CustomerName = customer.name
                        });
                    }
                }
                return roamservers;
            }
        }


        /// <summary>
        /// Gets the domain for a profile
        /// If multiple domains are configured, will return null
        /// </summary>
        /// <param name="profileId"></param>
        /// <param name="profileVersion"></param>
        /// <returns></returns>
        public async Task<String> GetProfileDomainAsync(Int32 profileId, String profileVersion)
        {
            var url = "https://openmobile.ipass.com/moservices/rest/api/ipass/1029590/mo/profileservice/configuration?id=" + profileId + ".V" + profileVersion + "&component=Accounts";
            var document = new XmlDocument();
            using (var client = await CreateAuthenticatedHttpClientAsync())
            {
                document.Load(await client.GetStreamAsync(url));

                //var d = XDocument.Load(await client.GetStreamAsync(url));
                //var masteraccount = d.Element("MasterAccount").Value;
                //var nodes = d.Elements("Account");
                //foreach (var node in nodes)
                //{
                //    node.Element()
                //}
            }



            var masteraccount = document.GetElementsByTagName("MasterAccount")[0].InnerText;

            var nodes = document.GetElementsByTagName("Account");
            foreach (XmlNode node in nodes)
            {
                if (node.Attributes["Name"].Value == masteraccount)
                {
                    var domains = node["Attributes"]["Domain"]["List"].ChildNodes;
                    return domains.Count == 1 ? domains[0].InnerText : null;
                }
            }
            return null;
        }


        /// <summary>
        /// Download ipass invoice
        /// </summary>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <returns></returns>
        public async Task<List<IpassInvoiceLineModel>> GetInvoiceAsync(DateTime from, DateTime to)
        {
            _log.Info("Getting ipass invoice url");
            var url = String.Format("https://openmobile.ipass.com/moservices/rest/api/ipass/1029590/mo/billing/invoicehistory?action=getInvoiceHeader&from={0}&to={1}", from.ToString("MM-dd-yyyy"), to.ToString("MM-dd-yyyy"));
            var document = new XmlDocument();
            using (var client = await CreateAuthenticatedHttpClientAsync())
            {
                document.LoadXml(await client.GetStringAsync(url));
            }

            // todo add handling of credit invoices...
            //if (document.GetElementsByTagName("invoiceDownloadUrl").Count != 1)
            //{
            //    throw new InvalidDataException("Unexpected number of invoices found, should be 1. Found " + document.GetElementsByTagName("invoiceDownloadUrl").Count + " Perhaps using incorrect timespan?");
            //}

            XmlNode node = document.GetElementsByTagName("invoiceDownloadUrl")[0];  // This will be the file name...
            var file = await GetFileAsync(node.InnerText);
            return ParseIpassInvoice(file.ToArray());
        }


        /// <summary>
        /// Parse an ipass pdf invoice and return the lines
        /// </summary>
        /// <param name="invoicePdf"></param>
        /// <returns></returns>
        private List<IpassInvoiceLineModel> ParseIpassInvoice(Byte[] invoicePdf)
        {
            _log.Info("Parsing ipass invoice");
            var rawtext = new StringBuilder();
            using (var pdfReader = new PdfReader(invoicePdf))
            {
                var strategy = new SimpleTextExtractionStrategy();
                for (int page = 1; page <= pdfReader.NumberOfPages; page++)
                {
                    var currentText = PdfTextExtractor.GetTextFromPage(pdfReader, page, strategy);
                    currentText = Encoding.UTF8.GetString(Encoding.Convert(Encoding.Default, Encoding.UTF8, Encoding.Default.GetBytes(currentText)));
                    rawtext.Append(currentText);
                }
            }

            var text = rawtext.ToString();
            var regex = new Regex("^[0-9]+\\s.*([0-9]{6,10}).*[0-9]+\\.[0-9]+", RegexOptions.Multiline);
            var matches = regex.Matches(text);
            var list = new List<IpassInvoiceLineModel>();
            foreach (Match match in matches)
            {
                var line = match.Value.Replace(Environment.NewLine, "");
                var r = new Regex("^([0-9]+)\\s(.*)\\s([0-9]{6,10})\\s(.*)\\s([0-9]*,?[0-9]+\\.[0-9]+)", RegexOptions.Multiline);
                var derp = r.Match(line).Groups;

                list.Add(new IpassInvoiceLineModel
                {
                    Number = Convert.ToInt32(derp[1].Value.Trim()),
                    Title = derp[2].Value.Trim(),
                    AccountNumber = Convert.ToInt32(derp[3].Value.Trim()),
                    AccountName = derp[4].Value.Trim(),
                    Sum = Convert.ToDecimal(derp[5].Value.Trim().Replace(",", "").Replace(".", ","))
                });
            }

            return list;
        }


        /// <summary>
        /// Download cdrs
        /// </summary>
        /// <param name="month"></param>
        /// <returns></returns>
        public async Task DownloadCdrsAsync(DateTime month)
        {
            var customers = await GetAccountsAsync();
            foreach (var customer in customers)
            {
                var url = $"https://openmobile.ipass.com/moservices/rest/api/ipass/{customer.customerId}/mo/cdrReports/monthly/cdr?month={month: yyyy-MM}";
                await ImportUsageAsync(url, month.ToString("yyyyMM"));
            }

            _log.Info("Download cdr done..." + Environment.NewLine);
        }


        /// <summary>
        /// Gets a file from the server
        /// </summary>
        /// <param name="customerPath">Path where the files are located</param>
        /// <param name="monthstringshort">yyyyMM</param>
        private async Task ImportUsageAsync(String customerPath, String monthstringshort)
        {
            try
            {
                _log.Info("Importing cdr file: " + customerPath);
                var sb = new StringBuilder();
                var regex = new Regex("(\"[0-9]+)(,)([0-9]+\")");
                using (var sr = new StreamReader(await GetFileAsync(customerPath)))
                {
                    while (sr.Peek() >= 0)
                    {
                        var line = sr.ReadLine() ?? "";
                        var match = regex.Match(line);
                        var duration = match.Groups[1].Value + match.Groups[3].Value;
                        line = regex.Replace(line, duration);
                        sb.AppendLine(line + ",\"" + monthstringshort + "\"");
                    }
                }

                // Parse small files and see if they contain the string "no data"
                // These files mess up the import. Big files are assumed to be valid CDRs
                // Assumption is the mother of all fuck ups...
                var filecontent = sb.ToString().Replace(",\"null\"", ",");
                if (filecontent.Length < 200 && (filecontent.Contains("no data") || filecontent.Contains("401 Authorization Required")))
                {
                    _log.Warn("Skipped cdr file: " + customerPath);
                    return;
                }

                // Convert the file contents string back a stream so it can be read again by csvreader...
                var contentbytes = Encoding.UTF8.GetBytes(filecontent);
                using (var contentstream = new MemoryStream(contentbytes))
                {
                    using (var connection = _contextFactory.GetSqlConnection())
                    {
                        var csvreader = new CsvReader(new StreamReader(contentstream), true, ',');
                        var bulkcopy = new SqlBulkCopy(connection)
                        {
                            DestinationTableName = "ipassusage",
                            BulkCopyTimeout = 60
                        };
                        bulkcopy.ColumnMappings.Add(0, "transaction_id");
                        bulkcopy.ColumnMappings.Add(1, "billingcode");
                        bulkcopy.ColumnMappings.Add(2, "username");
                        bulkcopy.ColumnMappings.Add(3, "realm");
                        bulkcopy.ColumnMappings.Add(4, "description");
                        bulkcopy.ColumnMappings.Add(5, "gmttime");
                        bulkcopy.ColumnMappings.Add(6, "localtime");
                        bulkcopy.ColumnMappings.Add(7, "sessionlength");
                        bulkcopy.ColumnMappings.Add(8, "billingrate");
                        bulkcopy.ColumnMappings.Add(9, "netbillingamount");
                        bulkcopy.ColumnMappings.Add(10, "accesstype");
                        bulkcopy.ColumnMappings.Add(11, "servicetype");
                        bulkcopy.ColumnMappings.Add(13, "dataused");
                        bulkcopy.ColumnMappings.Add(14, "month");

                        connection.Open();

                        // Veeeewwy veeewwy important! Otherwise decimal parsing from eg 5.53 will fail
                        Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
                        bulkcopy.WriteToServer(csvreader);
                    }
                }
            }
            catch (WebException wex)
            {
                _log.Warn($"Couldnt download file: {customerPath}\n{wex}\n");
            }
            catch (Exception hurr)
            {
                _log.Error($"Fubar file: {customerPath}\n{hurr.Message}");
            }
        }


        /// <summary>
        /// Gets the file and returns a local memory stream
        /// </summary>
        /// <param name="url"></param>
        /// <returns></returns>
        private async Task<MemoryStream> GetFileAsync(String url)
        {
            using (var client = await CreateAuthenticatedHttpClientAsync())
            {
                var stream = new MemoryStream();
                var response = await client.GetStreamAsync(url);
                await response.CopyToAsync(stream);
                stream.Position = 0;
                return stream;
            }
        }


        public async Task DownloadClientUsersAsync(DateTime month)
        {
            _log.Info("Importing client users");
            var monthstring = month.ToString("yyyyMM");
            var customers = await GetAccountsAsync();
            foreach (var customer in customers)
            {
                var url = $"https://openmobile.ipass.com/moservices/rest/api/ipass/{customer.customerId}/mo/cdrReports/monthly/networkuser?month={month: yyyy-MM}";
                await ImportClientUsersAsync(url, monthstring);
            }

            _log.Info("Done importing client users...\n");
        }


        private async Task ImportClientUsersAsync(String url, String month)
        {
            try
            {
                _log.Info("Importing client users file: " + url);

                //  "segoma1","btworld.net","No","Yes","No","201502"
                var sb = new StringBuilder();
                using (var sr = new StreamReader(await GetFileAsync(url)))
                {
                    while (sr.Peek() >= 0)
                    {
                        var line = sr.ReadLine() ?? "";
                        sb.AppendLine(line + ",\"" + month + "\"");
                    }
                }
                var filecontent = sb.ToString();
                var contentbytes = Encoding.UTF8.GetBytes(filecontent);
                using (var contentstream = new MemoryStream(contentbytes))
                {
                    using (var connection = _contextFactory.GetSqlConnection())
                    {
                        var bulkcopy = new SqlBulkCopy(connection)
                        {
                            DestinationTableName = "ipassusage_clientusers",
                            BulkCopyTimeout = 60
                        };
                        bulkcopy.ColumnMappings.Add(0, "username");
                        bulkcopy.ColumnMappings.Add(1, "realm");
                        bulkcopy.ColumnMappings.Add(5, "month");

                        var csvreader = new CsvReader(new StreamReader(contentstream), true, ',');

                        await connection.OpenAsync();
                        await bulkcopy.WriteToServerAsync(csvreader);
                    }
                }
            }
            catch (WebException wex)
            {
                _log.Warn("Couldnt download file: " + url + Environment.NewLine + wex.Message);
            }
            catch (Exception hurr)
            {
                _log.Warn("Fubar file: " + url + "\n" + hurr);
            }
        }


        /// <summary>
        /// Check a users credentials through netservers and roamservers etc
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="customerId"></param>
        /// <param name="roamserverIp"></param>
        /// <returns></returns>
        public async Task<XmlDocument> TestRoamserverAsync(String username, String password, Int32 customerId, String roamserverIp = null)
        {
            var content = $"<accessRequest><NAI>{username}</NAI><password>{password}</password><transactionServerHost>216.239.99.126</transactionServerHost><transactionServerPort>9101</transactionServerPort><classOfService>19</classOfService></accessRequest>";
            var url = $"https://openmobile.ipass.com/moservices/rest/api/ipass/{customerId}/mo/tools/authclient?action=authenticate&allRS=false";
            using (var client = await CreateAuthenticatedHttpClientAsync())
            {
                using (var response = await client.PostAsync(url, new StringContent(content)))
                {
                    var document = new XmlDocument();
                    document.Load(await response.Content.ReadAsStreamAsync());
                    return document;
                }
            }
        }
    }
}