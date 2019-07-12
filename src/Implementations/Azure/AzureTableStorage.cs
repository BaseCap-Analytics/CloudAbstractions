using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace BaseCap.CloudAbstractions.Implementations.Azure
{
    /// <summary>
    /// Provides a connection for manipulating Azure Storage Tables
    /// </summary>
    public class AzureTableStorage : ITableStorage
    {
        private const int MAX_BATCH_SIZE = 100;
        private const int MAX_ATTEMPTS = 5;
        private static readonly TimeSpan SERVER_TIMEOUT = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan RETRY_DELTA = TimeSpan.FromSeconds(1);
        private CloudTableClient _tables;
        private TableRequestOptions _options;

        /// <summary>
        /// Creates a new connection to Azure Table Storage
        /// </summary>
        public AzureTableStorage(string storageConnectionString)
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(storageConnectionString);
            _tables = account.CreateCloudTableClient();
            _options = new TableRequestOptions()
            {
                MaximumExecutionTime = SERVER_TIMEOUT,
                RetryPolicy = new Microsoft.WindowsAzure.Storage.RetryPolicies.LinearRetry(RETRY_DELTA, MAX_ATTEMPTS),
                ServerTimeout = SERVER_TIMEOUT,
            };
        }

        /// <summary>
        /// Creates a new connection to Azure Table Storage
        /// </summary>
        internal AzureTableStorage(CloudStorageAccount account)
        {
            _tables = account.CreateCloudTableClient();
            _options = new TableRequestOptions()
            {
                MaximumExecutionTime = SERVER_TIMEOUT,
                RetryPolicy = new Microsoft.WindowsAzure.Storage.RetryPolicies.LinearRetry(RETRY_DELTA, MAX_ATTEMPTS),
                ServerTimeout = SERVER_TIMEOUT,
            };
        }

        private async Task<CloudTable> GetTableReferenceAsync(string tableName)
        {
            CloudTable table = _tables.GetTableReference(tableName);
            await table.CreateIfNotExistsAsync(_options, null);
            return table;
        }

        /// <sumamry>
        /// Deletes the specified entry from the given table
        /// </summary>
        public async Task DeleteEntity<T>(T entity, string table) where T : TableEntity, new()
        {
            CloudTable tableRef = await GetTableReferenceAsync(table);
            TableOperation delete = TableOperation.Delete(entity);
            await tableRef.ExecuteAsync(delete, _options, null);
        }

        /// <sumamry>
        /// Deletes the specified entry from the given table only if the version of the entry is correct
        /// </summary>
        public async Task DeleteEntity(string id, string table, string etag = "*")
        {
            CloudTable tableRef = await GetTableReferenceAsync(table);
            TableEntity entity = await FindEntityByIdAsync<TableEntity>(id, table);
            await DeleteEntity<TableEntity>(entity, table);
        }

        /// <inheritdoc />
        public async Task DeleteOldEntitiesAsync(string table, string columnName, TimeSpan age, CancellationToken token)
        {
            CloudTable tableRef = await GetTableReferenceAsync(table);
            DateTimeOffset ageCutoff = DateTimeOffset.Now - age;
            TableQuery<DynamicTableEntity> selectPartitionKeyQuery = new TableQuery<DynamicTableEntity>()
            {
                FilterString = TableQuery.GenerateFilterConditionForDate(columnName, QueryComparisons.LessThan, ageCutoff),
                SelectColumns = new string[] { "PartitionKey", "RowKey" },
            };
            TableContinuationToken continuationToken = null;
            do
            {
                TableQuerySegment<DynamicTableEntity> segment = await tableRef.ExecuteQuerySegmentedAsync(selectPartitionKeyQuery, continuationToken, _options, null);
                continuationToken = segment.ContinuationToken;
                await DeleteInBatchAsync(tableRef, segment);
            }
            while ((continuationToken != null) && (token.IsCancellationRequested == false));
        }

        private async Task DeleteInBatchAsync(CloudTable table, TableQuerySegment<DynamicTableEntity> tableEntities)
        {
            Dictionary<string, TableBatchOperation> operations = new Dictionary<string, TableBatchOperation>();
            foreach (DynamicTableEntity item in tableEntities.Results)
            {
                string key = item.PartitionKey;
                if (operations.ContainsKey(key) == false)
                {
                    operations[key] = new TableBatchOperation();
                }

                operations[key].Add(TableOperation.Delete(item));

                if (operations[key].Count >= MAX_BATCH_SIZE)
                {
                    await table.ExecuteBatchAsync(operations[key]);
                    operations[key] = new TableBatchOperation();
                }
            }

            foreach (TableBatchOperation op in operations.Values)
            {
                if (op.Count > 0)
                {
                    await table.ExecuteBatchAsync(op);
                }
            }
        }

        /// <inheritdoc />
        public async Task DeleteTableAsync(string table)
        {
            CloudTable tableRef = await GetTableReferenceAsync(table);
            await tableRef.DeleteIfExistsAsync();
        }

        /// <summary>
        /// Queries the table for the single entry with the specified ID in the RowKey column
        /// </summary>
        public async Task<T> FindEntityByIdAsync<T>(string id, string table) where T : TableEntity, new()
        {
            CloudTable tableRef = await GetTableReferenceAsync(table);
            TableQuery<T> query = new TableQuery<T>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, id));
            TableQuerySegment<T> segment = await tableRef.ExecuteQuerySegmentedAsync(query, null, _options, null);
            if (segment.Results.Count == 0)
                return null;
            else if (segment.Results.Count == 1)
                return segment.Results[0];
            else
                throw new InvalidOperationException($"Expected 0 or 1 results from table '{table}'; got '{segment.Results.Count}'");
        }

        /// <summary>
        /// Queries the table for all the entries with the specified ID in the RowKey column
        /// </summary>
        public async Task<IEnumerable<T>> FindEntitiesByIdAsync<T>(string id, string table) where T : TableEntity, new()
        {
            CloudTable tableRef = await GetTableReferenceAsync(table);
            TableQuery<T> query = new TableQuery<T>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, id));
            List<T> entities = new List<T>();
            TableContinuationToken token = null;

            do
            {
                TableQuerySegment<T> segment = await tableRef.ExecuteQuerySegmentedAsync(query, token, _options, null);
                token = segment.ContinuationToken;
                entities.AddRange(segment.Results);
            }
            while (token != null);

            return entities;
        }

        /// <summary>
        /// Queries the table for all the entries with the specified ID in the given string-type column
        /// </summary>
        public async Task<IEnumerable<T>> FindEntitiesByColumnAsync<T>(string columnName, string value, string table) where T : TableEntity, new()
        {
            CloudTable tableRef = await GetTableReferenceAsync(table);
            TableQuery<T> query = new TableQuery<T>().Where(TableQuery.GenerateFilterCondition(columnName, QueryComparisons.Equal, value));
            List<T> entities = new List<T>();
            TableContinuationToken token = null;

            do
            {
                TableQuerySegment<T> segment = await tableRef.ExecuteQuerySegmentedAsync(query, token, _options, null);
                token = segment.ContinuationToken;
                entities.AddRange(segment.Results);
            }
            while (token != null);

            return entities;
        }

        /// <summary>
        /// Queries the table for all the entries with the specified ID in the given guid-type column
        /// </summary>
        public async Task<IEnumerable<T>> FindEntitiesByColumnAsync<T>(string columnName, Guid value, string table) where T : TableEntity, new()
        {
            CloudTable tableRef = await GetTableReferenceAsync(table);
            TableQuery<T> query = new TableQuery<T>().Where(TableQuery.GenerateFilterConditionForGuid(columnName, QueryComparisons.Equal, value));
            List<T> entities = new List<T>();
            TableContinuationToken token = null;

            do
            {
                TableQuerySegment<T> segment = await tableRef.ExecuteQuerySegmentedAsync(query, token, _options, null);
                token = segment.ContinuationToken;
                entities.AddRange(segment.Results);
            }
            while (token != null);

            return entities;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<string>> GetAllTableNamesAsync(CancellationToken token)
        {
            List<string> tableNames = new List<string>();
            TableContinuationToken continuationToken = null;

            do
            {
                TableResultSegment result = await _tables.ListTablesSegmentedAsync(continuationToken);
                continuationToken = result.ContinuationToken;
                tableNames.AddRange(result.Results.Select(t => t.Name));
            }
            while ((continuationToken != null) && (token.IsCancellationRequested == false));

            return tableNames;
        }

        /// <summary>
        /// Retrieves all entries in the specified table
        /// </summary>
        public async Task<IEnumerable<T>> GetAllEntitiesAsync<T>(string table) where T: TableEntity, new()
        {
            CloudTable tableRef = await GetTableReferenceAsync(table);
            TableQuery<T> query = new TableQuery<T>();
            List<T> entities = new List<T>();
            TableContinuationToken token = null;

            do
            {
                TableQuerySegment<T> segment = await tableRef.ExecuteQuerySegmentedAsync(query, token, _options, null);
                token = segment.ContinuationToken;
                entities.AddRange(segment.Results);
            }
            while (token != null);

            return entities;
        }

        /// <summary>
        /// Inserts a new entity into the specified table
        /// </summary>
        public async Task InsertEntityAsync<T>(T entity, string table) where T : TableEntity, new()
        {
            CloudTable tableRef = await GetTableReferenceAsync(table);
            TableOperation insert = TableOperation.Insert(entity);
            await tableRef.ExecuteAsync(insert, _options, null);
        }

        /// <summary>
        /// Inserts a batch of entities into the same table
        /// </summary>
        public async Task BulkInsertEntitiesAsync<T>(IEnumerable<T> entities, string table) where T : TableEntity, new()
        {
            TableBatchOperation batch = new TableBatchOperation();
            CloudTable tableRef = await GetTableReferenceAsync(table);

            foreach (T item in entities)
            {
                batch.Add(TableOperation.Insert(item));
            }

            await tableRef.ExecuteBatchAsync(batch, _options, null);
        }

        /// <summary>
        /// Updates a given entity given the specified version matches the version currently in the table
        /// </summary>
        public async Task UpdateEntityAsync<T>(string id, T newEntity, string table, string etag = "*") where T : TableEntity, new()
        {
            T oldEntity = await FindEntityByIdAsync<T>(id, table);
            if ((etag != "*") && (oldEntity.ETag != etag))
                throw new ArgumentOutOfRangeException($"The new Entity ETag must be the most current or '*'; got '{newEntity.ETag}'");

            newEntity.ETag = etag;
            CloudTable tableRef = await GetTableReferenceAsync(table);
            TableOperation update = TableOperation.Replace(newEntity);
            await tableRef.ExecuteAsync(update, _options, null);
        }

        /// <summary>
        /// Updates the given entity in a table without specifying an expected version
        /// </summary>
        public async Task UpdateEntityAsync<T>(T oldEntity, T newEntity, string table) where T : TableEntity, new()
        {
            if ((newEntity.ETag != "*") || (newEntity.ETag != oldEntity.ETag))
                throw new ArgumentOutOfRangeException($"The new Entity ETag must be the most current or '*'; got '{newEntity.ETag}'");

            CloudTable tableRef = await GetTableReferenceAsync(table);
            TableOperation update = TableOperation.Replace(newEntity);
            await tableRef.ExecuteAsync(update, _options, null);
        }

        /// <inheritdoc />
        public async Task<long> Count(string table)
        {
            long count = 0;
            CloudTable tableRef = await GetTableReferenceAsync(table);
            TableQuery<DynamicTableEntity> query = new TableQuery<DynamicTableEntity>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.NotEqual, string.Empty))
                .Select(new string[] { "PartitionKey" });
            EntityResolver<string> resolver = (pk, rk, ts, props, etag) => props.ContainsKey("PartitionKey") ? props["PartitionKey"].StringValue : null;
            TableContinuationToken token = null;

            do
            {
                TableQuerySegment<DynamicTableEntity> segment = await tableRef.ExecuteQuerySegmentedAsync(query, token, _options, null);
                token = segment.ContinuationToken;
                count += segment.Results.Count;
            }
            while (token != null);

            return count;
        }

        /// <inheritdoc />
        public async Task TraverseTableEntitiesAsync<T>(string tableName, Action<T> perEntityAction, CancellationToken cancelToken) where T : TableEntity, new()
        {
            CloudTable tableRef = await GetTableReferenceAsync(tableName);
            TableQuery<T> query = new TableQuery<T>();
            TableContinuationToken token = null;

            do
            {
                TableQuerySegment<T> segment = await tableRef.ExecuteQuerySegmentedAsync(query, token, _options, null, cancelToken);
                token = segment.ContinuationToken;
                segment.Results.ForEach(perEntityAction);
            }
            while (token != null);
        }
    }
}