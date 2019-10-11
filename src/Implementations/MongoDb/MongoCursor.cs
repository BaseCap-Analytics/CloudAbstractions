using BaseCap.CloudAbstractions.Abstractions;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.MongoDb
{
    /// <summary>
    /// A cursor into a MongoDB result set
    /// </summary>
    public class MongoCursor<T> : IDocumentDbCursor<T>
    {
        /// <inheritdoc />
        public IEnumerable<T> Current => _cursor!.Current;

        private readonly CancellationToken _token;
        private IAsyncCursor<T>? _cursor;

        /// <summary>
        /// Creates a new result set cursor to a MongoDB
        /// </summary>
        public MongoCursor(IAsyncCursor<T> cursor, CancellationToken token)
        {
            _cursor = cursor;
            _token = token;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_cursor != null && disposing)
            {
                _cursor.Dispose();
                _cursor = null;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <inheritdoc />
        public Task<bool> MoveNextAsync() => _cursor!.MoveNextAsync(_token);
    }
}
