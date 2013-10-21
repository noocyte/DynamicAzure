using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Xml;
using System.Xml.Linq;
using Newtonsoft.Json.Linq;

namespace Cyan
{
    public static class CyanSerializer
    {
        static readonly XNamespace defNamespace = "http://www.w3.org/2005/Atom";
        static readonly XNamespace dNamespace = "http://schemas.microsoft.com/ado/2007/08/dataservices";
        static readonly XNamespace mNamespace = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata";
        static readonly XDeclaration declaration = new XDeclaration("1.0", "utf-8", "yes");

        public static XDocument Serialize(this CyanEntity value)
        {
            var serializedProperties = value.GetFields()
                .Where(f => f.Key != "ETag")
                .Select(f => SerializeField(f.Key, f.Value))
                .ToArray();

            var document = SerializeDocument(serializedProperties);

            return document;
        }

        static XElement SerializeField(string name, object value)
        {
            var serialized = SerializeProperty(value);

            var serializedValue = serialized.Item1 == "Edm.String"
                ? new[] { serialized.Item2 }
                : new object[] { new XAttribute(mNamespace + "type", serialized.Item1), serialized.Item2 };

            var ret = new XElement(dNamespace + name, serializedValue);

            return ret;
        }

        static Tuple<string, string> SerializeProperty(object value)
        {
            var typeName = value.GetType().Name;

            string azureTypeName;
            string serialized;

            switch (typeName)
            {
                case "Byte[]":
                    azureTypeName = "Edm.Binary";
                    serialized = Convert.ToBase64String((byte[])value);
                    break;
                case "Boolean":
                    azureTypeName = "Edm.Boolean";
                    serialized = (bool)value ? "true" : "false";
                    break;
                case "DateTime":
                    azureTypeName = "Edm.DateTime";
                    serialized = XmlConvert.ToString((DateTime)value, XmlDateTimeSerializationMode.RoundtripKind);
                    break;
                case "Double":
                    azureTypeName = "Edm.Double";
                    serialized = value.ToString();
                    break;
                case "Guid":
                    azureTypeName = "Edm.Guid";
                    serialized = XmlConvert.ToString((Guid)value);
                    break;
                case "Int32":
                case "Integer":
                    azureTypeName = "Edm.Int32";
                    serialized = value.ToString();
                    break;
                case "Int64":
                    azureTypeName = "Edm.Int64";
                    serialized = value.ToString();
                    break;
                default:
                    azureTypeName = "Edm.String";
                    serialized = value.ToString();
                    break;
            }

            return new Tuple<string, string>(azureTypeName, serialized);
        }

        static XDocument SerializeDocument(XElement[] serializedProperties)
        {
            var document = new XDocument(
                   declaration,
                   new XElement(defNamespace + "entry",
                       new XAttribute(XNamespace.Xmlns + "d", dNamespace),
                       new XAttribute(XNamespace.Xmlns + "m", mNamespace),
                       new XAttribute("xmlns", defNamespace),
                       new XElement(defNamespace + "title"),
                       new XElement(defNamespace + "updated", XmlConvert.ToString(DateTime.UtcNow, XmlDateTimeSerializationMode.RoundtripKind)),
                       new XElement(defNamespace + "author",
                           new XElement(defNamespace + "name")),
                       new XElement(defNamespace + "id"),
                       new XElement(defNamespace + "content",
                           new XAttribute("type", "application/xml"),
                           new XElement(mNamespace + "properties", serializedProperties))));

            return document;
        }

        public static IEnumerable<dynamic> DeserializeEntities(XElement element)
        {
            return element
                .Elements(defNamespace + "entry")
                .Select(e => CyanSerializer.DeserializeEntity(e));
        }

        public static CyanEntity DeserializeEntity(XElement element)
        {
            var eTag = element.Attribute(mNamespace + "etag");

            var properties = element
                .Element(defNamespace + "content")
                .Element(mNamespace + "properties");

            var ret = new CyanEntity { ETag = eTag != null ? HttpUtility.UrlDecode(eTag.Value) : null };

            foreach (var item in properties.Elements())
            {
                var nullAttribute = item.Attribute(mNamespace + "null");
                if (nullAttribute != null && nullAttribute.Value == "true")
                    continue;

                switch (item.Name.LocalName)
                {
                    case "PartitionKey":
                        ret.PartitionKey = (string)DeserializeProperty(item);
                        break;
                    case "RowKey":
                        ret.RowKey = (string)DeserializeProperty(item);
                        break;
                    case "Timestamp":
                        ret.Timestamp = (DateTime)DeserializeProperty(item);
                        break;
                    default:
                        ret.Fields.Add(item.Name.LocalName, DeserializeProperty(item));
                        break;
                }
            }

            return ret;
        }

        static object DeserializeProperty(XElement propertyElement)
        {
            var typeAttribute = propertyElement.Attribute(mNamespace + "type");
            if (typeAttribute == null)
                return propertyElement.Value;

            var typeName = typeAttribute.Value;

            object ret;
            switch (typeName)
            {
                case "Edm.Binary":
                    ret = Convert.FromBase64String(propertyElement.Value);
                    break;
                case "Edm.Boolean":
                    ret = bool.Parse(propertyElement.Value);
                    break;
                case "Edm.DateTime":
                    ret = XmlConvert.ToDateTime(propertyElement.Value, XmlDateTimeSerializationMode.RoundtripKind);
                    break;
                case "Edm.Double":
                    ret = double.Parse(propertyElement.Value);
                    break;
                case "Edm.Guid":
                    ret = XmlConvert.ToGuid(propertyElement.Value);
                    break;
                case "Edm.Int32":
                    ret = int.Parse(propertyElement.Value);
                    break;
                case "Edm.Int64":
                    ret = long.Parse(propertyElement.Value);
                    break;
                default:
                    ret = propertyElement.Value;
                    break;
            }

            return ret;
        }
    }
}
