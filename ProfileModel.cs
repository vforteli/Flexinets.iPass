using System;

namespace Flexinets.iPass
{
    public class ProfileModel
    {        
       public int AccountId { get; set; }
       public DateTime? DateModified { get; set; }
       public string Name { get; set; }
       public string Pin { get; set; }
       public string Platform { get; set; }
       public int ProfileId { get; set; }
       public string ProfileVersion { get; set; }
       public string SoftwareVersion { get; set; }
       public string Status { get; set; }
    }
}