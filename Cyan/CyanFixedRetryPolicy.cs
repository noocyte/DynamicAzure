using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cyan
{
    public class CyanFixedRetryPolicy : CyanRetryPolicy
    {
        public CyanFixedRetryPolicy(int retries, TimeSpan interval)
        {
            this.retries = retries;
            this.interval = interval;
        }

        readonly int retries;
        readonly TimeSpan interval;

        public TimeSpan Interval { get { return interval; } }
        public int Retries { get { return retries; } }

        public override IEnumerable<TimeSpan> GetRetries()
        {
            for (int i = 0; i < retries; i++)
                yield return interval;
        }
    }
}
