using System;

namespace Flexinets.iPass
{
    public class UserStatusModel
    {
        public String UsernameDomain { get; set; }
        public double SuccessRate { get; set; }
        public DateTime LastAttempt { get; set; }
        public Int32 Sessions { get; set; }
        public Int32 Successes { get; set; }
        public Int32 Failures { get; set; }
    }
}
