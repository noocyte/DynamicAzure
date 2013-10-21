using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cyan
{
    public abstract class CyanRetryPolicy
    {
        public virtual bool ShouldRetry(Exception exception)
        {
            var cyanException = exception as CyanException;
            if (cyanException != null)
                return ShouldRetry(cyanException.ErrorCode);

            // should be a protocol error
            return true;
        }

        public virtual bool ShouldRetry(string errorCode)
        {
            switch (errorCode)
            {
                case "InternalError":
                case "OperationTimedOut":
                case "ServerBusy":
                case "TableBeingDeleted":
                    return true;
                default:
                    return false;
            }
        }

        public abstract IEnumerable<TimeSpan> GetRetries();

        public static readonly CyanRetryPolicy Default = new CyanNoRetriesRetryPolicy();

        public class CyanNoRetriesRetryPolicy : CyanRetryPolicy
        {
            public override bool ShouldRetry(Exception exception)
            {
                return false;
            }

            public override IEnumerable<TimeSpan> GetRetries()
            {
                yield break;
            }
        }
    }
}
