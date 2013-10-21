using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Xml.Linq;

namespace Cyan
{
    public class CyanBatchResponse : CyanRestResponse
    {
        CyanBatchResponse(HttpStatusCode statusCode,
            IEnumerable<KeyValuePair<string, string>> headers,
            XDocument responseBody)
            : base(statusCode, headers, responseBody)
        { }

        public IDictionary<string, CyanRestResponse> Responses { get; private set; }

        public override bool Succeeded
        {
            get
            {
                if (!base.Succeeded)
                    return false;

                return Responses.Values.All(r => r.Succeeded);
            }
        }

        public override bool PreconditionFailed
        {
            get
            {
                return Responses.Values.Any(r => r.PreconditionFailed);
            }
        }

        public override void ThrowIfFailed()
        {
            string failedId = null;
            try
            {
                var failed = Responses.FirstOrDefault(r => !r.Value.Succeeded);
                if (failed.Value != null)
                {
                    failedId = failed.Key;
                    failed.Value.ThrowIfFailed();
                }
            }
            catch (CyanException ex)
            {
                ex.Data.Add("ID", failedId);
                throw;
            }

            base.ThrowIfFailed();
        }

        public new static CyanBatchResponse Parse(HttpWebResponse response)
        {
            var statusCode = response.StatusCode;

            if (statusCode != HttpStatusCode.Accepted)
                CyanRestResponse.Parse(response).ThrowIfFailed();

            var headers = response.Headers.AllKeys
                .ToDictionary(k => k, k => response.Headers[k]);

            string batchBoundary;
            if (!TryParseBoundary(headers["Content-Type"], out batchBoundary))
                throw new ArgumentException("Invalid Content-Type in response.");

            string batchBody;
            var responses = ParseChangesets(response, batchBoundary, out batchBody);

            var respDictionary = responses.ToDictionary(r => r.Headers["Content-ID"]);

            XDocument responseBody = null;
            if (!string.IsNullOrWhiteSpace(batchBody))
                responseBody = XDocument.Parse(batchBody);

            return new CyanBatchResponse(statusCode, headers, responseBody) { Responses = respDictionary };
        }

        static CyanRestResponse[] ParseChangesets(HttpWebResponse response,
            string batchBoundary,
            out string batchBody)
        {
            var batchHeaders = new Dictionary<string, string>();

            var responses = new List<CyanRestResponse>();

            var batchBeginBoundary = string.Format("--{0}", batchBoundary);
            var batchEndBoundary = string.Format("--{0}--", batchBoundary);

            StringBuilder builder = null;
            string changesetBeginBoundary = null;
            string changesetEndBoundary = null;
            using (var responseStream = response.GetResponseStream())
            using (var reader = new StreamReader(responseStream))
            {
                ChangesetResponse currentChangeset = null;

                string currentLine;
                var state = ParserState.BeginResponse;
                while ((currentLine = reader.ReadLine()) != null)
                {
                    switch (state)
                    {
                        case ParserState.BeginResponse:
                            {
                                if (currentLine == batchBeginBoundary)
                                {
                                    state = ParserState.BatchHeaders;
                                    break;
                                }

                                if (!string.IsNullOrWhiteSpace(currentLine))
                                    throw new InvalidOperationException("Unexpected content.");

                                // empty line
                                break;
                            }
                        case ParserState.BatchHeaders:
                            {
                                Tuple<string, string> header;
                                if (TryParseHeader(currentLine, out header))
                                {
                                    batchHeaders.Add(header.Item1, header.Item2);
                                    if (header.Item1 == "Content-Type")
                                    {
                                        string changesetBoundary;
                                        if (TryParseBoundary(header.Item2, out changesetBoundary))
                                        {
                                            changesetBeginBoundary = string.Format("--{0}", changesetBoundary);
                                            changesetEndBoundary = string.Format("--{0}--", changesetBoundary);
                                        }
                                        else
                                        {
                                            throw new InvalidOperationException("Invalid batch Content-Type.");
                                        }
                                    }

                                    break;
                                }
                                else
                                {
                                    if (changesetBeginBoundary == null || changesetEndBoundary == null)
                                        throw new InvalidOperationException("Changeset boundary not found.");

                                    state = ParserState.BatchBody;
                                    break;
                                }
                            }
                        case ParserState.BatchBody:
                            {
                                if (currentLine == changesetBeginBoundary)
                                {
                                    currentChangeset = new ChangesetResponse();
                                    state = ParserState.ChangesetHeaders;
                                    break;
                                }

                                if (currentLine == batchEndBoundary)
                                {
                                    if (builder != null)
                                    {
                                        // the batch failed, parse the error
                                    }

                                    state = ParserState.EndResponse;
                                    break;
                                }

                                if (!string.IsNullOrWhiteSpace(currentLine))
                                {
                                    // the batch failed, collect the error
                                    if (builder == null)
                                        builder = new StringBuilder();
                                    builder.AppendLine(currentLine);
                                }
                                break;
                            }
                        case ParserState.ChangesetHeaders:
                            {
                                Tuple<string, string> header;
                                if (TryParseHeader(currentLine, out header))
                                {
                                    currentChangeset.changesetHeaders.Add(header.Item1, header.Item2);
                                    break;
                                }
                                else
                                {
                                    state = ParserState.ResponseStatusLine;
                                    break;
                                }
                            }
                        case ParserState.ResponseStatusLine:
                            {
                                var statusTokens = currentLine.Split(' ');
                                if (statusTokens.Length < 2)
                                    throw new InvalidOperationException("Malformed status line.");

                                var httpVersion = statusTokens[0];
                                var unparsedStatusCode = statusTokens[1];
                                int parsedStatusCode;
                                if (!int.TryParse(unparsedStatusCode, out parsedStatusCode))
                                    throw new InvalidOperationException("Invalid status code.");

                                currentChangeset.responseStatus = (HttpStatusCode)parsedStatusCode;

                                state = ParserState.ResponseHeaders;
                                break;
                            }
                        case ParserState.ResponseHeaders:
                            {
                                Tuple<string, string> header;
                                if (TryParseHeader(currentLine, out header))
                                {
                                    currentChangeset.responseHeaders.Add(header.Item1, header.Item2);
                                    break;
                                }
                                else
                                {
                                    state = ParserState.ResponseBody;
                                    break;
                                }
                            }
                        case ParserState.ResponseBody:
                            {
                                bool changesetEnd = false;
                                if (currentLine == changesetBeginBoundary
                                    || (changesetEnd = currentLine == changesetEndBoundary))
                                {
                                    // todo: return old changeset
                                    var changesetResponse = currentChangeset.ToResponse();

                                    responses.Add(changesetResponse);

                                    currentChangeset = changesetEnd ? null : new ChangesetResponse();
                                    state = changesetEnd ? ParserState.BatchBody : ParserState.ChangesetHeaders;
                                    break;
                                }

                                currentChangeset.responseBuilder.AppendLine(currentLine);
                                break;
                            }
                    }
                }

                if (state != ParserState.EndResponse)
                    throw new InvalidOperationException("Premature end of response.");
            }

            var body = builder != null ? builder.ToString() : null;
            batchBody = !string.IsNullOrWhiteSpace(body) ? body : null;

            return responses.ToArray();
        }

        class ChangesetResponse
        {
            public Dictionary<string, string> changesetHeaders = new Dictionary<string, string>();
            public Dictionary<string, string> responseHeaders = new Dictionary<string, string>();
            public HttpStatusCode responseStatus;
            public StringBuilder responseBuilder = new StringBuilder();

            public CyanRestResponse ToResponse()
            {
                var responseBody = responseBuilder.ToString();

                bool hasBody = responseStatus != HttpStatusCode.NoContent;

                var ret = new CyanRestResponse(responseStatus,
                    responseHeaders,
                    hasBody ? XDocument.Parse(responseBody) : null);

                return ret;
            }
        }

        enum ParserState
        {
            BeginResponse,
            BatchHeaders,
            BatchBody,
            ChangesetHeaders,
            ResponseStatusLine,
            ResponseHeaders,
            ResponseBody,
            EndResponse
        }

        static bool TryParseHeader(string text, out Tuple<string, string> header)
        {
            header = null;
            if (string.IsNullOrWhiteSpace(text))
                return false;

            var index = text.IndexOf(':');
            if (index <= 0)
                return false;

            header = Tuple.Create(text.Substring(0, index), text.Substring(index + 1).Trim());
            return true;
        }

        static bool TryParseBoundary(string contentType, out string boundary)
        {
            boundary = null;
            var headerTokens = contentType.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(t => t.Trim())
                .ToArray();

            if (headerTokens.Length < 2 || headerTokens[0] != "multipart/mixed")
                return false;

            var boundaryToken = headerTokens[1];
            var boundarySeparatorIndex = boundaryToken.IndexOf('=');
            var leftPart = boundaryToken.Substring(0, boundarySeparatorIndex);
            var rightPart = boundaryToken.Substring(boundarySeparatorIndex + 1);

            if (leftPart != "boundary")
                return false;

            boundary = rightPart;
            return true;
        }
    }
}
