using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// The contract for interacting with cloud-based non-sql tables
    /// </summary>
    public interface ITableStorage
    {
        /// <summary>
        /// Inserts a new entity into table storage
        /// </summary>
        /// <param name="entity">The entity to insert</param>
        /// <param name-"table">The name of the table to insert into</param>
        Task InsertEntityAsync<T>(T entity, string table) where T: TableEntity, new();

        /// <summary>
        /// Inserts a batch of entities into the same table
        /// </summary>
        /// <param name="entities">The entities to insert</param>
        /// <param name-"table">The name of the table to insert into</param>
        Task BulkInsertEntitiesAsync<T>(IEnumerable<T> entities, string table) where T: TableEntity, new();

        /// <summary>
        /// Finds a specific entity in table storage by a unique ID and returns it
        /// </summary>
        /// <param name="id">The unique identifier of the entity to find</param>
        /// <param name="table">The name of the table to search</param>
        /// <returns>Returns the entity, if it was found; returns null if the entity was not found</returns>
        Task<T> FindEntityByIdAsync<T>(string id, string table) where T: TableEntity, new();

        /// <summary>
        /// Retrieves all entities in the specified table with the specified string filter
        /// </summary>
        Task<IEnumerable<T>> FindEntitiesByColumnAsync<T>(string columnName, string value, string table) where T : TableEntity, new();

        /// <summary>
        /// Retrieves all entities in the specified table with the specified Guid filter
        /// </summary>
        Task<IEnumerable<T>> FindEntitiesByColumnAsync<T>(string columnName, Guid value, string table) where T : TableEntity, new();

        /// <summary>
        /// Retrieves all stored entities in a table
        /// </summary>
        /// <param name="table">The name of the table to search</param>
        /// <returns>Returns an enumeration of all entities in the table; returns null for an empty table</returns>
        Task<IEnumerable<T>> GetAllEntitiesAsync<T>(string table) where T: TableEntity, new();

        /// <summary>
        /// Updates a stored entity with new data
        /// </summary>
        /// <param name="id">The ID of the entity to update</param>
        /// <param name="newEntity">The new data to store</param>
        /// <param name="table">The table to update</param>
        /// <param name="etag">The etag of the entity, used to prevent stale update conflicts</param>
        Task UpdateEntityAsync<T>(string id, T newEntity, string table, string etag = "*") where T: TableEntity, new();

        /// <summary>
        /// Updates a stored entity with new data
        /// </summary>
        /// <param name="oldEntity">The old entity to be overwritten</param>
        /// <param name="newEntity">The new data to write</param>
        /// <param name="table">The table to update</param>
        Task UpdateEntityAsync<T>(T oldEntity, T newEntity, string table) where T: TableEntity, new();

        /// <summary>
        /// Deletes the specified entity from the table
        /// </summary>
        /// <param name="entity">The entity to remove from the table</param>
        /// <param name="table">The table to remove the entity from</param>
        Task DeleteEntity<T>(T entity, string table) where T: TableEntity, new();

        /// <summary>
        /// Deletes the specified entity from the table
        /// </summary>
        /// <param name="id">The unique identifier of the entity to remove from the table</param>
        /// <param name="table">The table to remove the entity from</param>
        /// <param name="etag">The etag of the entity to remove; used to prevent stale data deletes</param>
        Task DeleteEntity(string id, string table, string etag = "*");

        /// <summary>
        /// Retrieves the number of rows in the specified table
        /// </summary>
        /// <param name="table">The table to query on</param>
        /// <returns>Returns an awaitable Task to retrieve the count</returns>
        Task<long> Count(string table);

        /// <summary>
        /// Traverses all entities in a Table and executes an action on them.
        /// Results of this action will not be saved
        /// </summary>
        /// <param name="tableName">The name of the table to traverse</param>
        /// <param name="perEntityAction">The action to take on each entity</param>
        /// <returns>Returns an awaitable Task</returns>
        Task TraverseTableEntitiesAsync<T>(string tableName, Action<T> perEntityAction) where T : TableEntity, new();
    }
}
