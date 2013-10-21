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
                    var p = subObject.Parent.Parent.Parent;
                    subObject.Add("PartitionKey", ((dynamic)p).RowKey);
                    JToken idToken;
                    if (subObject.TryGetValue("id", out idToken))
                    {
                        subObject.Add("RowKey", idToken.Value<string>());
                        subObject.Remove("id");
                    }
                    else
                    {
                        subObject.Add("RowKey", Guid.NewGuid().ToString()); // TODO UXID!
                    }

                    // "events": [{ "id": "XI1BTWMDXR6X", ... }, { "id": "OK1JAWL7WWQS", ... }]
                    foreignKeys.Add(String.Format("{0}_{1}", prop.Name, subObject.GetValue("RowKey").Value<string>()), ConvertToSimpleObject(subObject));
                }
            }
        }

        public static CyanEntity ConvertToSimpleObject(JObject obj)
        {
            var ce = new CyanEntity();

            int numberOfChildElements = 0;

            foreach (var prop in obj.Properties())
            {

                if (prop.Value is JArray)
                {
                    ce.Fields.Add(String.Format("ChildTable_{0}", numberOfChildElements), prop.Name);
                    numberOfChildElements++;
                    continue;
                }

                switch (prop.Value.Type.ToString())
                {
                    case "Boolean":
                        ce.Fields.Add(prop.Name, prop.Value.Value<bool>());
                        break;
                    case "Float":
                        ce.Fields.Add(prop.Name, prop.Value.Value<double>());
                        break;
                    case "Integer":
                        ce.Fields.Add(prop.Name, prop.Value.Value<int>());
                        break;
                    case "String":
                        ce.Fields.Add(prop.Name, prop.Value.Value<string>());
                        break;
                    case "Date":
                        ce.Fields.Add(prop.Name, prop.Value.Value<DateTime>());
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
