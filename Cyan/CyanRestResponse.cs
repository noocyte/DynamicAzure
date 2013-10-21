using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Xml.Linq;

namespace Cyan
{
    public class CyanRestResponse
    {
        public static CyanRestResponse Parse(HttpWebResponse response)
        {
            var statusCode = response.StatusCode;
            var headers = response.Headers.AllKeys.Select(n => new KeyValuePair<string, string>(n, response.Headers[n]));

            string body = null;
            using (var responseStream = response.GetResponseStream())
            using (var reader = new StreamReader(responseStream))
                body = reader.ReadToEnd();

            XDocument parsedBody = !string.IsNullOrEmpty(body) ? XDocument.Parse(body) : null;

            return new CyanRestResponse(statusCode, headers, parsedBody);
        }

        internal CyanRestResponse(HttpStatusCode statusCode, IEnumerable<KeyValuePair<string, string>> headers, XDocument responseBody)
        {
            StatusCode = statusCode;
            Headers = headers.ToDictionary(h => h.Key, h => h.Value);
            ResponseBody = responseBody;
        }

        public HttpStatusCode StatusCode { get; private set; }
        public IDictionary<string, string> Headers { get; private set; }
        public XDocument ResponseBody { get; private set; }
        public virtual bool Succeeded
        {
            get
            {
                return (int)StatusCode < 300;
            }
        }

        public virtual bool PreconditionFailed
        {
            get
            {
                return StatusCode == HttpStatusCode.PreconditionFailed;
            }
        }

        public virtual void ThrowIfFailed()
        {
            if (!Succeeded)
            {
                throw CyanException.Parse(ResponseBody, StatusCode);
            }
        }

        public override string ToString()
        {
            return string.Format("{0} ({1})", (int)StatusCode, StatusCode);
        }
    }
}
