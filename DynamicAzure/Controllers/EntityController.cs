using Cyan;
using Newtonsoft.Json.Linq;
using System;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace DynamicAzure.Controllers
{
    public class EntityController : ApiController
    {
        CyanClient TableClient = new CyanClient("dynamicazure", "NWEBI6mTJihjtv2VqTNV2ImcVAJ6uc1mqz097C2qLINuHdApWvQ4woO/mNGHE4H5pt4ISzCfGTODc9wEUq2Ckw==");

        // GET api/entity
        public HttpResponseMessage Get(string client, string entity)
        {
            var tablename = VerifyTable(client, entity);
            var table = GetTable(tablename);

            return Request.CreateResponse(HttpStatusCode.OK, table.Query("PartitionKey"));
        }

        // GET api/entity/5
        public HttpResponseMessage Get(string client, string entity, string id)
        {
            var tablename = VerifyTable(client, entity);
            var table = GetTable(tablename);

            return Request.CreateResponse(HttpStatusCode.OK, table.Query("PartitionKey", id));
        }

        // PUT api/entity/5
        public HttpResponseMessage Put(string id, string client, string entity, dynamic dynamicObj)
        {
            var tablename = VerifyTable(client, entity);
            var table = GetTable(tablename);

            InsertObject(id, dynamicObj, table);
            return Respond(client, entity, dynamicObj, id);
        }

        // POST api/entity
        public HttpResponseMessage Post(string client, string entity, dynamic dynamicObj)
        {
            var tablename = VerifyTable(client, entity);
            var table = GetTable(tablename);

            var rowKey = Guid.NewGuid().ToString(); // get proper UXID!
            InsertObject(rowKey, dynamicObj, table);

            return Respond(client, entity, dynamicObj, rowKey);
        }

        private CyanTable GetTable(string tablename)
        {
            var table = TableClient[tablename];
            return table;
        }

        private string VerifyTable(string client, string entity)
        {
            var tablename = string.Format("{0}{1}", client, entity);
            TableClient.TryCreateTable(tablename);
            return tablename;
        }

        private HttpResponseMessage Respond(string client, string entity, dynamic dynamicObj, string rowKey)
        {
            string uri = Url.Link("DefaultApi", new { id = rowKey, controller = "Entity", client = client, entity = entity });
            var response = Request.CreateResponse(HttpStatusCode.Created, dynamicObj as JObject);
            response.Headers.Location = new Uri(uri);
            return response;
        }

        private static void InsertObject(string id, dynamic dynamicObj, CyanTable table)
        {
            table.Insert(new
            {
                PartitionKey = "PartitionKey",
                RowKey = id,
                Name = dynamicObj.Name.Value,
                Age = dynamicObj.Age.Value
            });
        }
    }
}
