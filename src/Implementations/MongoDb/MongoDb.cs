using BaseCap.CloudAbstractions.Abstractions;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.MongoDb
{
    /// <summary>
    /// Creates a Connection to a collection inside MongoDB
    /// </summary>
    public class MongoDb<T> : IDocumentDb<T>
    {
        private readonly MongoClient _client;
        private readonly IMongoDatabase _db;
        private IMongoCollection<T>? _collection;

        /// <summary>
        /// Creates a new MongoDB connection
        /// </summary>
        /// <param name="serverAddress">The URL of the server to connect to</param>
        /// <param name="serverPort">The port to connect on</param>
        /// <param name="username">The authentication user name</param>
        /// <param name="password">The authentication password</param>
        /// <param name="database">The MongoDB database name to use</param>
        public MongoDb(string serverAddress, ushort serverPort, string username, string password, string database)
        {
            _client = new MongoClient(new MongoClientSettings()
            {
                ConnectTimeout = TimeSpan.FromSeconds(30),
                Credential = MongoCredential.CreateCredential(database, username, password),
                RetryReads = true,
                RetryWrites = true,
                Server = new MongoServerAddress(serverAddress, serverPort),
            });
            _db = _client.GetDatabase(database, new MongoDatabaseSettings()
            {
                WriteConcern = WriteConcern.W1,
            });
        }

        /// <inheritdoc />
        public async Task CreateCollectionAsync(
            string name,
            IEnumerable<Expression<Func<T, object>>>? ascendingIndexes,
            IEnumerable<Expression<Func<T, object>>>? descendingIndexes,
            Expression<Func<T, object>> expireyIndex,
            TimeSpan ttl)
        {
            if (_collection != null)
            {
                throw new InvalidOperationException($"Must not call {nameof(UseExistingCollection)} or {nameof(CreateCollectionAsync)} before calling {nameof(CreateCollectionAsync)}");
            }

            // Create the collection
            await _db.CreateCollectionAsync(name).ConfigureAwait(false);
            _collection = _db.GetCollection<T>(name);

            // Set the ascending indexes
            if ((ascendingIndexes != null) && ascendingIndexes.Any())
            {
                foreach (Expression<Func<T, object>> index in ascendingIndexes)
                {
                    await _collection.Indexes.CreateOneAsync(new CreateIndexModel<T>(new IndexKeysDefinitionBuilder<T>().Ascending(index))).ConfigureAwait(false);
                }
            }

            // Set the descending indexes
            if ((descendingIndexes != null) && descendingIndexes.Any())
            {
                foreach (Expression<Func<T, object>> index in descendingIndexes)
                {
                    await _collection.Indexes.CreateOneAsync(new CreateIndexModel<T>(new IndexKeysDefinitionBuilder<T>().Descending(index))).ConfigureAwait(false);
                }
            }

            // Set the expiration period
            await _collection.Indexes.CreateOneAsync(
                new CreateIndexModel<T>(
                    new IndexKeysDefinitionBuilder<T>().Ascending(expireyIndex),
                    new CreateIndexOptions()
                    {
                        ExpireAfter = ttl
                    })).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public void UseExistingCollection(string name)
        {
            if (_collection != null)
            {
                throw new InvalidOperationException($"Must not call {nameof(UseExistingCollection)} or {nameof(CreateCollectionAsync)} before calling {nameof(UseExistingCollection)}");
            }

            _collection = _db.GetCollection<T>(name);
        }

        /// <inheritdoc />
        public async Task<IDocumentDbCursor<T>> FindEntitiesAsync(Expression<Func<T, bool>> whereClause, CancellationToken token)
        {
            if (_collection == null)
            {
                throw new InvalidOperationException($"Must call {nameof(UseExistingCollection)} or {nameof(CreateCollectionAsync)} before calling {nameof(FindEntityAsync)}");
            }

            IFindFluent<T, T> query = _collection.Find(whereClause);
            IAsyncCursor<T> nativeCursor = await query.ToCursorAsync(token).ConfigureAwait(false);
            return new MongoCursor<T>(nativeCursor, token);
        }

        /// <inheritdoc />
        public async Task<IDocumentDbCursor<T>> FindEntitiesAscendingAsync(
            Expression<Func<T, bool>> whereClause,
            Expression<Func<T, object>> orderByClause,
            int maxCount,
            CancellationToken token)
        {
            if (_collection == null)
            {
                throw new InvalidOperationException($"Must call {nameof(UseExistingCollection)} or {nameof(CreateCollectionAsync)} before calling {nameof(FindEntityAsync)}");
            }

            IFindFluent<T, T> query = _collection.Find(whereClause)
                                                 .SortBy(orderByClause)
                                                 .Limit(maxCount);
            IAsyncCursor<T> nativeCursor = await query.ToCursorAsync(token).ConfigureAwait(false);
            return new MongoCursor<T>(nativeCursor, token);
        }

        /// <inheritdoc />
        public async Task<IDocumentDbCursor<T>> FindEntitiesDescendingAsync(
            Expression<Func<T, bool>> whereClause,
            Expression<Func<T, object>> orderByClause,
            int maxCount,
            CancellationToken token)
        {
            if (_collection == null)
            {
                throw new InvalidOperationException($"Must call {nameof(UseExistingCollection)} or {nameof(CreateCollectionAsync)} before calling {nameof(FindEntityAsync)}");
            }

            IFindFluent<T, T> query = _collection.Find(whereClause)
                                                 .SortByDescending(orderByClause)
                                                 .Limit(maxCount);
            IAsyncCursor<T> nativeCursor = await query.ToCursorAsync(token).ConfigureAwait(false);
            return new MongoCursor<T>(nativeCursor, token);
        }

        /// <inheritdoc />
        public Task<T> FindEntityAsync(Expression<Func<T, bool>> whereClause, CancellationToken token)
        {
            if (_collection == null)
            {
                throw new InvalidOperationException($"Must call {nameof(UseExistingCollection)} or {nameof(CreateCollectionAsync)} before calling {nameof(FindEntityAsync)}");
            }

            return _collection.Find(whereClause).SingleOrDefaultAsync(token);
        }

        ///<inheritdoc />
        public Task InsertEntitiesAsync(IEnumerable<T> entities, CancellationToken token)
        {
            if (_collection == null)
            {
                throw new InvalidOperationException($"Must call {nameof(UseExistingCollection)} or {nameof(CreateCollectionAsync)} before calling {nameof(InsertEntitiesAsync)}");
            }

            return _collection.InsertManyAsync(entities, cancellationToken: token);
        }

        /// <inheritdoc />
        public Task<long> EntityCountAsync(Expression<Func<T, bool>> whereClause, CancellationToken token)
        {
            if (_collection == null)
            {
                throw new InvalidOperationException($"Must call {nameof(UseExistingCollection)} or {nameof(CreateCollectionAsync)} before calling {nameof(EntityCountAsync)}");
            }

            return _collection.CountDocumentsAsync(whereClause, cancellationToken: token);
        }
    }
}
