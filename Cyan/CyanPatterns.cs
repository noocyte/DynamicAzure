using System;
using System.Collections.Generic;
using System.Linq;

namespace Cyan
{
    public static class CyanPatterns
    {
        public static dynamic InsertOrUpdate(this CyanTable table,
            string partitionKey,
            string rowKey,
            Func<object> entityProvider,
            Action<dynamic> entityModifier)
        {
            dynamic entity;
            var success = false;
            do
            {
                entity = table.Query(partitionKey, rowKey).FirstOrDefault();

                // if the entity does not exist TryInsert
                if (entity == null)
                {
                    // if we succeed creation the loop is finished
                    success = table.TryInsert(entityProvider(), out entity);

                    // if we fail
                    // someone must have created it in the meanwhile
                    // retry the update
                    continue;
                }

                // the entity exists, update its fields
                entityModifier(entity);

                // try to commit the update
                success = table.TryUpdate(entity);

                // if !success someone must have inserted or updated
                // the entity concurrently
            } while (!success);

            return entity;
        }

        public static dynamic InsertOrMerge(this CyanTable table,
            string partitionKey,
            string rowKey,
            Func<object> entityProvider,
            Action<dynamic> entityModifier, params string[] fields)
        {
            dynamic entity;
            var success = false;
            do
            {
                entity = table.Query(partitionKey, rowKey).FirstOrDefault();

                // if the entity does not exist TryInsert
                if (entity == null)
                {
                    // if we succeed creation the loop is finished
                    success = table.TryInsert(entityProvider(), out entity);

                    // if we fail
                    // someone must have created it in the meanwhile
                    // retry the update
                    continue;
                }

                // the entity exists, update its fields
                entityModifier(entity);

                // try to commit the update
                success = table.TryMerge(entity, fields);

                // if !success someone must have inserted or updated
                // the entity concurrently
            } while (!success);

            return entity;
        }

        public static dynamic Update(this CyanTable table,
            string partitionKey,
            string rowKey,
            Action<dynamic> entityModifier)
        {
            var success = false;
            dynamic entity;
            do
            {
                entity = table.Query(partitionKey, rowKey).FirstOrDefault();

                entityModifier(entity);

                success = table.TryUpdate(entity);
            } while (!success);

            return entity;
        }

        public static IEnumerable<dynamic> BatchUpdate(this CyanTable table,
            Func<CyanTable, IEnumerable<dynamic>> entityModifier,
            bool unconditionalUpdate = false)
        {
            var success = false;
            List<dynamic> ret = null;
            do
            {
                var toUpdate = entityModifier(table);
                ret = new List<dynamic>();

                var batch = table.Batch();
                foreach (var item in toUpdate)
                    ret.Add(batch.Update(item, unconditionalUpdate));

                success = batch.TryCommit();
            } while (!success);

            return ret;
        }

        public static void BatchDelete(this CyanTable table, IEnumerable<object> entities, bool unconditionalUpdate = false)
        {
            table.BatchDelete(entities.Select(e => CyanEntity.FromObject(e)), unconditionalUpdate);
        }

        public static void BatchDelete(this CyanTable table, IEnumerable<CyanEntity> entities, bool unconditionalUpdate = false)
        {
            foreach (var partition in GetBatchPartitions(entities))
            {
                if (partition.Length == 1)
                {
                    // no need for a batch request
                    table.Delete(partition[0], unconditionalUpdate);
                }
                else
                {
                    var batch = table.Batch();
                    foreach (var entity in partition)
                        batch.Delete(entity, unconditionalUpdate);

                    batch.Commit();
                }
            }
        }

        public static IEnumerable<dynamic> BatchInsert(this CyanTable table, IEnumerable<object> entities)
        {
            return table.BatchInsert(entities.Select(e => CyanEntity.FromObject(e)));
        }

        public static IEnumerable<dynamic> BatchInsert(this CyanTable table, IEnumerable<CyanEntity> entities)
        {
            foreach (var partition in GetBatchPartitions(entities))
            {
                if (partition.Length == 1)
                {
                    // no need for a batch request
                    yield return table.Insert(partition[0]);
                }
                else
                {
                    List<dynamic> ret = new List<dynamic>();
                    var batch = table.Batch();
                    foreach (var entity in partition)
                        ret.Add(batch.Insert(entity));

                    batch.Commit();

                    foreach (var item in ret)
                        yield return item;
                }
            }
        }

        public static void Empty(this CyanTable table)
        {
            var entities = table.Query();
            table.BatchDelete(entities);
        }

        static IEnumerable<CyanEntity[]> GetBatchPartitions(IEnumerable<CyanEntity> entities)
        {
            Dictionary<string, CyanEntity> batch = null;
            string currentPartition = null;
            foreach (var entity in entities)
            {
                // batch supports operations on the same partition key only
                // batch supports max 100 operations
                // assume that items with the same partition are sequential
                // (when obtained via Query() they are)
                if (batch != null
                    && (entity.PartitionKey != currentPartition || batch.Count == 100))
                {
                    yield return batch.Values.ToArray();
                    batch = null;
                    currentPartition = null;
                }

                if (currentPartition == null)
                    currentPartition = entity.PartitionKey;

                if (batch == null)
                    batch = new Dictionary<string, CyanEntity>();

                if (batch.ContainsKey(entity.PartitionKey))
                    throw new NotSupportedException("Cannot batch multiple operations on the same Entity.");

                batch.Add(entity.RowKey, entity);
            }

            if (batch != null)
                yield return batch.Values.ToArray();
        }
    }
}
