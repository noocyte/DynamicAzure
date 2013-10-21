using System;
using System.Collections.Generic;
using System.Net;
using System.Xml.Linq;

namespace Cyan
{
    [Serializable]
    public class CyanException : Exception
    {
        public CyanException() { }
        public CyanException(HttpStatusCode statusCode, string errorCode, string message, XDocument responseBody, IDictionary<string, string> data)
            : base(message)
        {
            this.StatusCode = statusCode;
            this.ErrorCode = errorCode;
            this.ResponseBody = responseBody;
        }

        //public CyanException(string message, Exception inner) : base(message, inner) { }
        protected CyanException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context)
            : base(info, context) { }

        public static CyanException Parse(CyanRestResponse response)
        {
            return Parse(response.ResponseBody, response.StatusCode);
        }

        public static CyanException Parse(XDocument responseBody, HttpStatusCode statusCode)
        {
            string code = null;
            string message = null;

            Dictionary<string, string> data = new Dictionary<string, string>();
            foreach (var element in responseBody.Root.Elements())
            {
                switch (element.Name.LocalName)
                {
                    case "code":
                        code = element.Value;
                        break;
                    case "message":
                        message = element.Value;
                        break;
                    default:
                        if (!data.ContainsKey(element.Name.LocalName))
                            data.Add(element.Name.LocalName, element.Value);
                        break;
                }
            }

            return new CyanException(statusCode, code, message, responseBody, data);
        }

        public HttpStatusCode StatusCode { get; set; }
        public string ErrorCode { get; set; }
        public XDocument ResponseBody { get; set; }
    }
}
