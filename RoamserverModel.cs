using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Flexinets.iPass
{
    public class RoamserverModel
    {
        public String CompanyId;
        public String IpAddress;
        public Int32 Port;
        public Int32 Priority;
        public String CustomerName;
        public DateTime DateModified;
    }
}

/*
<serverInfo>
    <companyId>1036769</companyId>
    <connectionName>139.122.200.249</connectionName>
    <id>61794</id>
    <ipAddress>139.122.200.249</ipAddress>
    <port>577</port>
    <priority>2</priority>
    <serverType>ROAM_SERV</serverType>
    <acctIdleTimeout>30000</acctIdleTimeout>
    <connSharing>0</connSharing>
    <description>Ticket ID:86316</description>
    <idleTimeout>120000</idleTimeout>
    <modifiedBy>1</modifiedBy>
    <modifiedDate>2013-09-28T02:17:41Z</modifiedDate>
    <numRetry>0</numRetry>
    <retryRsDelay>15</retryRsDelay>
    <email>philharmonic@trial.ipass.com</email>
    <firstName>iPass</firstName>
    <lastName>Admin</lastName>
    <CustomerName>Scania Infomate AB</CustomerName>
  </serverInfo>
*/