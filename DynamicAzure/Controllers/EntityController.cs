using Cyan;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
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
            var tablename = string.Format("{0}{1}", client, entity);

            // get a table reference (this does not make any request)
            var table = TableClient[tablename];

            return Request.CreateResponse(HttpStatusCode.OK, table.Query("PartitionKey"));
        }

        // GET api/entity/5
        public HttpResponseMessage Get(string client, string entity, string id)
        {
            var tablename = string.Format("{0}{1}", client, entity);

            // get a table reference (this does not make any request)
            var table = TableClient[tablename];

            return Request.CreateResponse(HttpStatusCode.OK, table.Query("PartitionKey", id));
        }

        // PUT api/entity/5
        public HttpResponseMessage Put(string id, string client, string entity, dynamic dynamicObj)
        {
            // make sure the table has been created
            var tablename = string.Format("{0}{1}", client, entity);
            TableClient.TryCreateTable(tablename);

            // get a table reference (this does not make any request)
            var table = TableClient[tablename];

            // insert an entity
            table.Insert(new
            {
                PartitionKey = "PartitionKey",
                RowKey = id,
                Name = dynamicObj.Name.Value,
                Age = dynamicObj.Age.Value
            });

            string uri = Url.Link("DefaultApi", new { id = id, controller = "Entity", client = client, entity = entity });
            var response = Request.CreateResponse(HttpStatusCode.Created, dynamicObj as JObject);
            response.Headers.Location = new Uri(uri);
            return response;
        }

        // POST api/entity
        public HttpResponseMessage Post(string client, string entity, dynamic dynamicObj)
        {
            // make sure the table has been created
            var tablename = string.Format("{0}{1}", client, entity);
            TableClient.TryCreateTable(tablename);

            // get a table reference (this does not make any request)
            var table = TableClient[tablename];

            // insert an entity
            var rowKey = Guid.NewGuid().ToString(); // get proper UXID!
            table.Insert(new
            {
                PartitionKey = "PartitionKey",
                RowKey = rowKey,
                Name = dynamicObj.Name.Value,
                Age = dynamicObj.Age.Value
            });

            string uri = Url.Link("DefaultApi", new { id = rowKey, controller = "Entity", client = client, entity = entity });
            var response = Request.CreateResponse(HttpStatusCode.Created, dynamicObj as JObject);
            response.Headers.Location = new Uri(uri);
            return response;
        }

        // DELETE api/entity/5
        public void Delete(int id)
        {
        }
    }
}
