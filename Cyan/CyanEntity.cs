using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace Cyan
{
    public class CyanEntity : DynamicObject
    {
        public string ETag { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTime Timestamp { get; set; }

        public readonly Dictionary<string, object> Fields = new Dictionary<string, object>();

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            switch (binder.Name)
            {
                case "PartitionKey":
                    result = PartitionKey;
                    break;
                case "RowKey":
                    result = RowKey;
                    break;
                case "Timestamp":
                    result = Timestamp;
                    break;
                default:
                    Fields.TryGetValue(binder.Name, out result);
                    break;
            }

            return true;
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            var ret = true;
            var name = binder.Name;

            switch (name)
            {
                case "PartitionKey":
                case "RowKey":
                case "Timestamp":
                    ret = false;
                    break;
                default:
                    if (value == null)
                    {
                        if (Fields.ContainsKey(name))
                            Fields.Remove(name);
                    }
                    else
                    {
                        Fields[name] = value;
                    }
                    break;
            }

            return ret;
        }

        public override IEnumerable<string> GetDynamicMemberNames()
        {
            if (PartitionKey != null)
                yield return "PartitionKey";

            if (RowKey != null)
                yield return "RowKey";

            if (Timestamp != default(DateTime))
                yield return "Timestamp";

            foreach (var key in Fields.Keys)
                yield return key;
        }

        public override string ToString()
        {
            return string.Format("Partition: {0}, Row: {1}", PartitionKey, RowKey);
        }

        internal IEnumerable<KeyValuePair<string, object>> GetFields()
        {
            if (PartitionKey != null)
                yield return new KeyValuePair<string, object>("PartitionKey", PartitionKey);

            if (RowKey != null)
                yield return new KeyValuePair<string, object>("RowKey", RowKey);

            if (ETag != null)
                yield return new KeyValuePair<string, object>("ETag", RowKey);

            foreach (var item in Fields)
                yield return item;
        }

        static internal CyanEntity FromEntity(CyanEntity entity, params string[] fields)
        {
            var filterFields = new HashSet<string>(fields.GroupBy(f => f).Select(g => g.Key));

            var ret = new CyanEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = entity.ETag,
                Timestamp = entity.Timestamp
            };

            var filteredFields = entity.Fields.Where(f => filterFields.Contains(f.Key));
            foreach (var field in filteredFields)
                ret.Fields.Add(field.Key, field.Value);

            return ret;
        }

        public static CyanEntity FromObject(object value)
        {
            CyanEntity ret = value as CyanEntity;
            if (ret != null)
                return ret;

            // get object name/values via reflection
            var properties = value
                .GetType()
                .GetProperties()
                .Select(f => new KeyValuePair<string, object>(f.Name, f.GetValue(value, null)));

            return FromEnumerable(properties);
        }

        public static CyanEntity FromDictionary(IDictionary<string, object> dictionary)
        {
            return FromEnumerable(dictionary);
        }

        public static CyanEntity FromEnumerable(IEnumerable<KeyValuePair<string, object>> enumerable)
        {
            CyanEntity ret = new CyanEntity();

            foreach (var field in enumerable)
            {
                switch (field.Key)
                {
                    case "PartitionKey":
                        ret.PartitionKey = field.Value as string;
                        if (ret.PartitionKey == null)
                            throw new ArgumentException("PartitionKey must be of type \"System.String\".");
                        CyanUtilities.ValidateKeyField(ret.PartitionKey);
                        break;
                    case "RowKey":
                        ret.RowKey = field.Value as string;
                        if (ret.RowKey == null)
                            throw new ArgumentException("RowKey must be of type \"System.String\".");
                        CyanUtilities.ValidateKeyField(ret.RowKey);
                        break;
                    case "ETag":
                        ret.ETag = field.Value as string;
                        if (ret.ETag == null)
                            throw new ArgumentException("ETag must be of type \"System.String\".");
                        break;
                    default:
                        CyanUtilities.ValidateFieldType(field.Value.GetType());
                        ret.Fields.Add(field.Key, field.Value);
                        break;
                }
            }

            return ret;
        }
    }
}
