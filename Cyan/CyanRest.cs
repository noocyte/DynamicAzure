using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Threading;

namespace Cyan
{
    [DebuggerDisplay("CyanRest({accountName})")]
    public class CyanRest
    {
        public CyanRest(string accountName, string accountSecret, bool useSsl = false, CyanRetryPolicy retryPolicy = null)
        {
            UseSsl = useSsl;
            RetryPolicy = retryPolicy ?? CyanRetryPolicy.Default;

            account = new CyanAccount(accountName, accountSecret);
        }

        CyanAccount account;

        public bool UseSsl { get; private set; }

        public string AccountName { get { return account.Name; } }

        public bool IsDevelopmentStorage { get { return AccountName == CyanClient.developmentStorageAccount; } }

        public CyanRetryPolicy RetryPolicy { get; private set; }

        public CyanRestResponse GetRequest(string resource, string query = null)
        {
            return Request("GET", resource, query: query);
        }

        public CyanRestResponse PostRequest(string resource, string content)
        {
            return Request("POST", resource, content: content);
        }

        public CyanRestResponse PutRequest(string resource, string content, string ifMatch = null)
        {
            return Request("PUT", resource, content: content, ifMatch: ifMatch);
        }

        public CyanRestResponse MergeRequest(string resource, string content, string ifMatch = null)
        {
            return Request("MERGE", resource, content: content, ifMatch: ifMatch);
        }

        public CyanRestResponse DeleteRequest(string resource, string ifMatch = null)
        {
            return Request("DELETE", resource, ifMatch: ifMatch ?? "*");
        }

        public CyanBatchResponse BatchRequest(string multipartBoundary, byte[] contentBytes)
        {
            var response = GetResponse("POST",
                "$batch",
                contentType: string.Format("multipart/mixed; boundary={0}", multipartBoundary),
                contentBytes: contentBytes);

            return CyanBatchResponse.Parse(response);
        }

        CyanRestResponse Request(string method,
            string resource,
            string query = null,
            string versionHeader = null,
            string contentType = null,
            string content = null,
            byte[] contentBytes = null,
            string ifMatch = null)
        {
            CyanRestResponse ret = null;

            IEnumerator<TimeSpan> retries = RetryPolicy.GetRetries().GetEnumerator();
            bool retry;
            do
            {
                retry = false;
                try
                {
                    using (var response = GetResponse(method,
                        resource,
                        query: query,
                        contentType: contentType,
                        content: content,
                        contentBytes: contentBytes,
                        ifMatch: ifMatch))
                        ret = CyanRestResponse.Parse(response);
                }
                catch (Exception ex)
                {
                    if (RetryPolicy.ShouldRetry(ex) && retries.MoveNext())
                    {
                        retry = true;
                        Thread.Sleep(retries.Current);
                    }

                    if (!retry)
                        throw;
                }

                if (!retry
                    && !ret.Succeeded
                    && retries.MoveNext()
                    && RetryPolicy.ShouldRetry(CyanException.Parse(ret)))
                {
                    retry = true;
                    Thread.Sleep(retries.Current);
                }
            } while (retry);

            return ret;
        }

        HttpWebResponse GetResponse(string method,
            string resource,
            string query = null,
            string contentType = null,
            string content = null,
            byte[] contentBytes = null,
            string ifMatch = null)
        {
            var url = FormatUrl(resource, query);

            if (contentBytes == null)
            {
                // encode request content
                contentBytes = !string.IsNullOrEmpty(content) ? Encoding.UTF8.GetBytes(content) : null;
            }

            var request = (HttpWebRequest)WebRequest.Create(url);
            request.Method = method;

            // required headers
            request.ContentType = contentType ?? "application/atom+xml";
            request.Headers.Add("DataServiceVersion", "2.0;NetFx");
            request.Headers.Add("MaxDataServiceVersion", "2.0;NetFx");
            request.Headers.Add("x-ms-date", XMsDate);
            request.Headers.Add("x-ms-version", "2011-08-18");

            if (!string.IsNullOrEmpty(ifMatch))
                request.Headers.Add("If-Match", ifMatch);

            // sign the request
            account.Sign(request);

            try
            {
                if (contentBytes != null)
                {
                    // we have some content to send
                    request.ContentLength = contentBytes.Length;

                    using (var requestStream = request.GetRequestStream())
                        requestStream.Write(contentBytes, 0, contentBytes.Length);
                }

                return (HttpWebResponse)request.GetResponse();
            }
            catch (WebException webEx)
            {
                // if ProtocolError (ie connection problems) throw
                // in this case I should probably implement a retry policy
                if (webEx.Status != WebExceptionStatus.ProtocolError)
                    throw;

                // we have a response from the service
                return (HttpWebResponse)webEx.Response;
            }
        }

        public string FormatUrl(string resource, string query = null)
        {
            if (IsDevelopmentStorage)
            {
                // development storage url http://127.0.0.1:10002/devstoreaccount1/{resource}?{query}
                var url = string.Format("http://127.0.0.1:10002/{0}/{1}", AccountName, resource);
                if (!string.IsNullOrEmpty(query))
                    url = string.Join("?", url, query);

                return url;
            }
            else
            {
                // table storage {protocol}://{account}.table.core.windows.net/{resource}?{query}
                var protocol = UseSsl ? "https" : "http";

                var url = string.Format("{0}://{1}.table.core.windows.net/{2}", protocol, AccountName, resource);
                if (!string.IsNullOrEmpty(query))
                    url = string.Join("?", url, query);

                return url;
            }
        }

        /// <summary>
        /// Returns the current time formatted for the storage requests.
        /// </summary>
        static string XMsDate
        {
            get
            {
                return DateTime.UtcNow.ToString("R", System.Globalization.CultureInfo.InvariantCulture);
            }
        }
    }
}
