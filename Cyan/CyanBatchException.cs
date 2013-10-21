using System;

namespace Cyan
{
    [Serializable]
    public class CyanBatchException : Exception
    {
        public CyanBatchException() { }
        public CyanBatchException(string message) : base(message) { }
        public CyanBatchException(string message, Exception inner) : base(message, inner) { }
        protected CyanBatchException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context)
            : base(info, context) { }

        public string ContentID { get; set; }
    }
}
