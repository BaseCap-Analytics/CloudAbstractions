using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// A read-cursor into a result set from the document db
    /// </summary>
    public interface IDocumentDbCursor<T> : IDisposable
    {
        /// <summary>
        /// The current batch of document results
        /// </summary>
        IEnumerable<T> Current { get; }

        /// <summary>
        /// Moves to the next result batch
        /// </summary>
        /// <returns>Returns true when there are more result batches to be processed</returns>
        Task<bool> MoveNextAsync();
    }
}
