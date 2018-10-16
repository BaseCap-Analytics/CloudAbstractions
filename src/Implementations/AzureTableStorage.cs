using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;

namespace BaseCap.CloudAbstractions.Implementations
{
    /// <summary>
    /// Provides a connection for manipulating Azure Storage Tables
    /// </summary>
    public class AzureTableStorage : ITableStorage
    {
        private static readonly TimeSpan TIMEOUT = TimeSpan.FromSeconds(30);
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
                MaximumExecutionTime = TIMEOUT,
                RetryPolicy = new Microsoft.WindowsAzure.Storage.RetryPolicies.ExponentialRetry(),
                ServerTimeout = TIMEOUT,
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
                MaximumExecutionTime = TIMEOUT,
                RetryPolicy = new Microsoft.WindowsAzure.Storage.RetryPolicies.ExponentialRetry(),
                ServerTimeout = TIMEOUT,
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
    }
}
