using Cyan;
using DynamicAzure.Models;
using Newtonsoft.Json.Linq;
using System;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace DynamicAzure.Controllers
{
    public class EntityController : ApiController
    {
        readonly CyanClient TableClient = new CyanClient("dynamicazure", "NWEBI6mTJihjtv2VqTNV2ImcVAJ6uc1mqz097C2qLINuHdApWvQ4woO/mNGHE4H5pt4ISzCfGTODc9wEUq2Ckw==");
        internal string Client { get; set; }

        // GET api/entity
        public HttpResponseMessage Get(string client, string entity)
        {
            this.Client = client;
            var table = GetTable(entity);

            return Request.CreateResponse(HttpStatusCode.OK, table.Query("PartitionKey"));
        }

        // GET api/entity/5
        public HttpResponseMessage Get(string client, string entity, string id)
        {
            this.Client = client;
            var table = GetTable(entity);

            return Request.CreateResponse(HttpStatusCode.OK, table.Query("PartitionKey", id));
        }

        // PUT api/entity/5
        public HttpResponseMessage Put(string id, string client, string entity, dynamic dynamicObj)
        {
            this.Client = client;
            var table = GetTable(entity);

            InsertObject(id, dynamicObj, table);
            return Respond(client, entity, dynamicObj, id);
        }

        // POST api/entity
        public HttpResponseMessage Post(string client, string entity, dynamic dynamicObj)
        {
            this.Client = client;
            var table = GetTable(entity);

            var rowKey = Guid.NewGuid().ToString(); // get proper UXID!
            InsertObject(rowKey, dynamicObj, table);

            return Respond(client, entity, dynamicObj, rowKey);
        }

        private CyanTable GetTable(string entity)
        {
            var tablename = string.Format("{0}{1}", this.Client, entity);
            TableClient.TryCreateTable(tablename);
            var table = TableClient[tablename];

            return table;
        }

        private HttpResponseMessage Respond(string client, string entity, dynamic dynamicObj, string rowKey)
        {
            string uri = Url.Link("DefaultApi", new { id = rowKey, controller = "Entity", client = client, entity = entity });
            var response = Request.CreateResponse(HttpStatusCode.Created, dynamicObj as JObject);
            response.Headers.Location = new Uri(uri);
            return response;
        }

        private void InsertObject(string id, dynamic dynamicObj, CyanTable currentTable)
        {
            var objs = JsonSubObjectsTraverser.Traverse(dynamicObj as JObject);

            var simpleObject = JsonSubObjectsTraverser.ConvertToSimpleObject(dynamicObj);

            simpleObject.PartitionKey = "PK";
            simpleObject.RowKey = id;

            currentTable.Insert(simpleObject);
           
            foreach (var entity in objs.Keys)
            {
                var entityId = entity.Split('_')[1];
                var enityName = entity.Split('_')[0];
                var table = GetTable(enityName);
                simpleObject = objs[entity];

                simpleObject.PartitionKey = id;
                simpleObject.RowKey = entityId;

                table.Insert(simpleObject);
            }
        }
    }
}
