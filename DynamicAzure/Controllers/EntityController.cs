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
        // GET api/entity
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET api/entity/5
        public string Get(int id)
        {
            return "value";
        }

        // POST api/entity
        public void Post(string client, string entity, JObject value)
        {
            var tableClient = new CyanClient("dynamicazure", "NWEBI6mTJihjtv2VqTNV2ImcVAJ6uc1mqz097C2qLINuHdApWvQ4woO/mNGHE4H5pt4ISzCfGTODc9wEUq2Ckw==");

            // make sure the table has been created
            var tablename = string.Format("{0}{1}", client, entity);
            tableClient.TryCreateTable(tablename);

            // get a table reference (this does not make any request)
            var table = tableClient[tablename];

            // insert an entity
            var rowKey = Guid.NewGuid().ToString();
            table.Insert(new
            {
                PartitionKey = "PartitionKey",
                RowKey = rowKey,
                MyField = "foo bar",
                MyIntField = 1337
            });

            // get an entity
            var entityVal = table.Query("PartitionKey", rowKey).First();

            // update
            entityVal.MyField = "new value";
            table.Update(entityVal);

            // delete
            table.Delete(entityVal);
        }

        // PUT api/entity/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/entity/5
        public void Delete(int id)
        {
        }
    }
}
