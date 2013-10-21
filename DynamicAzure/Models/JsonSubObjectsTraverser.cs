using Cyan;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace DynamicAzure.Models
{
    /// <summary>
    /// Allows you to traverse a JSON object to find Foreign Keys
    /// </summary>
    public static class JsonSubObjectsTraverser
    {
        public static Dictionary<string, dynamic> Traverse(JObject jsonObject)
        {
            var foreignKeys = new Dictionary<string, dynamic>();
            TraverseToken(foreignKeys, jsonObject);
            return foreignKeys;
        }

        private static void TraverseToken(IDictionary<string, dynamic> foreignKeys, JToken token)
        {
            foreach (var childToken in token.Children())
            {
                if (childToken is JProperty)
                {
                    var prop = childToken as JProperty;
                    GetValue(foreignKeys, prop);
                }

                TraverseToken(foreignKeys, childToken);
            }
        }

        private static void GetValue(IDictionary<string, dynamic> foreignKeys, JProperty prop)
        {
            var array = prop.Value as JArray;
            if (array == null) return;

            foreach (var subObj in array)
            {
                var subObject = subObj as JObject;

                if (subObject != null)
                {
                    // "events": [{ "id": "XI1BTWMDXR6X", ... }, { "id": "OK1JAWL7WWQS", ... }]
                    foreignKeys.Add(String.Format("{0}_{1}", prop.Name, subObject.GetValue("id").Value<string>()), ConvertToSimpleObject(subObject));
                }
            }
        }

        public static CyanEntity ConvertToSimpleObject(JObject obj)
        {
            var ce = new CyanEntity();

            foreach (var prop in obj.Properties())
            {
                if (prop.Value is JArray)
                    continue;

                switch (prop.Name)
                {
                    case "PartitionKey":
                    case "RowKey":
                    case "Timestamp":
                    case "ETag":
                        break;
                    default:
                        ce.Fields.Add(prop.Name, prop.Value.Value<object>());
                        break;
                }
            }

            return ce;
        }
    }
}
