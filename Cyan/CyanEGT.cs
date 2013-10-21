using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Web;

namespace Cyan
{
    public class CyanEGT
    {
        internal CyanEGT(CyanTable table)
        {
            this.table = table;
        }

        CyanTable table;
        string partitionKey;
        HashSet<string> modifiedRows = new HashSet<string>();
        List<EntityOperation> operations = new List<EntityOperation>();

        public dynamic Insert(object entity)
        {
            var cyanEntity = CyanEntity.FromObject(entity);

            AddOperation(cyanEntity,
                "POST",
                table.TableName);

            return cyanEntity;
        }

        public void InsertOrUpdate(object entity)
        {
            var cyanEntity = CyanEntity.FromObject(entity);

            AddOperation(cyanEntity,
                "PUT",
                table.FormatResource(cyanEntity.PartitionKey, cyanEntity.RowKey));
        }

        public void Update(object entity, bool unconditionalUpdate = false)
        {
            var cyanEntity = CyanEntity.FromObject(entity);

            AddOperation(cyanEntity,
                "PUT",
                table.FormatResource(cyanEntity.PartitionKey, cyanEntity.RowKey),
                Tuple.Create("If-Match", unconditionalUpdate ? "*" : cyanEntity.ETag));
        }

        public void Merge(object entity, bool unconditionalUpdate = false)
        {
            var cyanEntity = CyanEntity.FromObject(entity);

            AddOperation(cyanEntity,
                "MERGE",
                table.FormatResource(cyanEntity.PartitionKey, cyanEntity.RowKey),
                Tuple.Create("If-Match", unconditionalUpdate ? "*" : cyanEntity.ETag));
        }

        public void Delete(object entity, bool unconditionalUpdate = false)
        {
            var cyanEntity = CyanEntity.FromObject(entity);

            Delete(cyanEntity.PartitionKey, cyanEntity.RowKey, unconditionalUpdate ? null : cyanEntity.ETag);
        }

        public void Delete(string partitionKey, string rowKey, string eTag = null)
        {
            AddOperation(partitionKey,
                rowKey,
                "DELETE",
                table.FormatResource(partitionKey, rowKey),
                Tuple.Create("If-Match", eTag == null ? "*" : eTag));
        }

        public void Commit()
        {
            var batchBoundary = string.Format("batch_{0}", Guid.NewGuid());
            var requestBody = EncodeBatchRequestBody(batchBoundary);

            var debug = Encoding.UTF8.GetString(requestBody);

            var response = table.restClient.BatchRequest(batchBoundary, requestBody);

            response.ThrowIfFailed();

            foreach (var operationResponse in response.Responses)
            {
                var index = int.Parse(operationResponse.Key);
                EntityOperation op = operations[index];

                string eTagHeader;
                // update entity etag
                if (operationResponse.Value.Headers.TryGetValue("ETag", out eTagHeader))
                    op.UpdateEntityETag(HttpUtility.UrlDecode(eTagHeader));
            }
        }

        public bool TryCommit()
        {
            if (operations.Count == 0)
                return true;

            var batchBoundary = string.Format("batch_{0}", Guid.NewGuid());
            var requestBody = EncodeBatchRequestBody(batchBoundary);

            var debug = Encoding.UTF8.GetString(requestBody);

            var response = table.restClient.BatchRequest(batchBoundary, requestBody);

            if (response.StatusCode != HttpStatusCode.Accepted)
                response.ThrowIfFailed();

            var failedPrecondition = response.Responses.Values.FirstOrDefault(r => r.PreconditionFailed);
            if (failedPrecondition != null)
                return false;

            response.ThrowIfFailed();

            foreach (var operationResponse in response.Responses)
            {
                var index = int.Parse(operationResponse.Key);
                EntityOperation op = operations[index];

                string eTagHeader;
                // update entity etag
                if (operationResponse.Value.Headers.TryGetValue("ETag", out eTagHeader))
                    op.UpdateEntityETag(eTagHeader);
            }

            return true;
        }

        byte[] EncodeBatchRequestBody(string batchBoundary)
        {
            var changesetBoundary = string.Format("changeset_{0}", Guid.NewGuid());

            byte[] contentBytes = null;

            using (var contentStream = new EGTRequestStream())
            {
                // write batch boundary
                contentStream.WriteBoundary(batchBoundary);
                // write batch Content-Type header
                contentStream.WriteHeader("Content-Type", string.Format("multipart/mixed; boundary={0}", changesetBoundary));
                // blank line after headers
                contentStream.WriteLine();

                int index = 0;
                foreach (var operation in operations)
                {
                    // each changeset
                    // write changeset begin boundary
                    contentStream.WriteBoundary(changesetBoundary);

                    // required headers
                    contentStream.WriteHeader("Content-Type", "application/http");
                    contentStream.WriteHeader("Content-Transfer-Encoding", "binary");
                    contentStream.WriteLine();

                    // write changeset payload
                    operation.Write(contentStream, table.restClient, index++.ToString());
                }

                // write changeset and batch end boundaries
                contentStream.WriteEndBoundary(changesetBoundary);
                contentStream.WriteEndBoundary(batchBoundary);

                contentBytes = contentStream.ToArray();
            }

            //var debug = Encoding.UTF8.GetString(contentBytes);

            return contentBytes;
        }

        void ValidateEntity(string partitionKey, string rowKey)
        {
                if (partitionKey == null)
                    throw new ArgumentNullException("partitionKey");
                if (rowKey == null)
                    throw new ArgumentNullException("rowKey");

            if (this.partitionKey == null)
            {
                this.partitionKey = partitionKey;
            }
            else
            {
                if (this.partitionKey != partitionKey)
                    throw new ArgumentException("Invalid partition key.", "partitionKey");
            }

            if (modifiedRows.Contains(rowKey))
            {
                throw new NotSupportedException("Multiple operations on the same entity are not supported in the same batch.");
            }
            else
            {
                modifiedRows.Add(rowKey);
            }
        }

        void AddOperation(string partitionKey,
                string rowKey,
                string method,
                string resource,
                params Tuple<string, string>[] headers)
        {
            ValidateEntity(partitionKey, rowKey);

            operations.Add(EntityOperation.CreateOperation(null, method, resource, headers));
        }

        void AddOperation(CyanEntity entity,
                string method,
                string resource,
                params Tuple<string, string>[] headers)
        {
            ValidateEntity(entity.PartitionKey, entity.RowKey);

            operations.Add(EntityOperation.CreateOperation(entity, method, resource, headers));
        }

        class EntityOperation
        {
            public static EntityOperation CreateOperation(CyanEntity entity,
                string method,
                string resource,
                params Tuple<string, string>[] headers)
            {
                if (string.IsNullOrEmpty(method))
                    throw new ArgumentNullException("method");
                if (string.IsNullOrEmpty(resource))
                    throw new ArgumentNullException("resource");

                if (headers == null)
                    headers = new Tuple<string, string>[0];

                var ret = new EntityOperation
                {
                    entity = entity,
                    method = method,
                    resource = resource,
                    headers = headers
                };

                return ret;
            }

            EntityOperation() { }

            IEnumerable<Tuple<string, string>> headers;
            string method;
            string resource;
            CyanEntity entity;

            public void UpdateEntityETag(string eTag)
            {
                if ((method == "POST" || method == "PUT" || method == "MERGE") && entity != null)
                    entity.ETag = eTag;
            }

            public void Write(EGTRequestStream requestStream, CyanRest restClient, string contentId)
            {
                byte[] contentBytes = null;
                if (entity != null)
                {
                    var content = entity.Serialize();
                    contentBytes = Encoding.UTF8.GetBytes(content.ToString());
                }

                var finalHeaders = new List<Tuple<string, string>>();
                finalHeaders.Add(Tuple.Create("Content-ID", contentId));
                if (contentBytes != null && contentBytes.Length > 0)
                {
                    finalHeaders.Add(Tuple.Create("Content-Type", "application/atom+xml;type=entry"));
                    finalHeaders.Add(Tuple.Create("Content-Length", contentBytes.Length.ToString()));
                }

                if (headers != null)
                    finalHeaders.AddRange(headers);

                // write status line
                requestStream.WriteLine("{0} {1} {2}", method, restClient.FormatUrl(resource), "HTTP/1.1");

                // write headers
                foreach (var header in finalHeaders)
                    requestStream.WriteHeader(header.Item1, header.Item2);
                requestStream.WriteLine();

                // write content
                if (contentBytes != null)
                {
                    requestStream.Write(contentBytes, 0, contentBytes.Length);
                    requestStream.WriteLine();
                }
            }
        }
    }
}
