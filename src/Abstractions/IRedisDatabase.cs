using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Redis.Database
{
    /// <summary>
    /// Interface describing the actions that can be taken on a Redis Database
    /// </summary>
    public interface IRedisDatabase
    {
        /// <summary>
        /// Sends the HELLO command to the server (v5 and above)
        /// </summary>
        /// <param name="token">A cancellation token to cancel the operation</param>
        /// <returns>Returns a map of configuration information about the server</returns>
        ValueTask<Dictionary<string, string>> HelloAsync(CancellationToken token);

        /// <summary>
        /// Sends the HSET command to the server to set one or more hash fields on a key
        /// </summary>
        /// <param name="keyName">The name of the Key for the entry in the database</param>
        /// <param name="hashValues">The hash field name/value pairs to set</param>
        /// <param name="token">A cancellation token to cancel the operation</param>
        /// <returns>Returns the number of entries that were set</returns>
        ValueTask<long> HSetAsync(string keyName, Dictionary<string, string> hashValues, CancellationToken token);

        /// <summary>
        /// Sends the HGET command to retrieve the value from a specific hash field
        /// </summary>
        /// <param name="keyName">The name of the Key for the entry in the database</param>
        /// <param name="fieldName">The name of the hash field</param>
        /// <param name="token">A cancellation token to cancel the operation</param>
        /// <typeparam name="T">The type of the value in the Hash Field</typeparam>
        /// <returns>Returns an object of type <typeparamref name="T">, if possible; otherwise, returns null</retuens>
        [return: System.Diagnostics.CodeAnalysis.MaybeNull]
        ValueTask<T> HGetAsync<T>(string keyName, string fieldName, CancellationToken token);

        /// <summary>
        /// Sends the HGET command to retrieve the deserialized object value in the specific hash field
        /// </summary>
        /// <param name="keyName">The name of the Key for the entry in the database</param>
        /// <param name="fieldName">The name of the hash field</param>
        /// <param name="token">A cancellation token to cancel the operation</param>
        /// <typeparam name="T">The type of the object serialized in the Hash Field</typeparam>
        /// <returns>Returns the deserialized object, if possible; otherwise, returns null</returns>
        ValueTask<T?> HGetSerializedAsync<T>(string keyName, string fieldName, CancellationToken token) where T : class;

        /// <summary>
        /// Sends the HINCRBY command in order to increment a value in a hash field
        /// </summary>
        /// <param name="keyName">The name of the Key for the entry in the database</param>
        /// <param name="fieldName">The name of the hash field</param>
        /// <param name="token">A cancellation token to cancel the operation</param>
        /// <param name="increment">The amount to increment the hash field by</param>
        /// <returns>Returns the new value in the hash field</returns>
        ValueTask<long> HIncrementByAsync(string keyName, string fieldName, CancellationToken token, long increment = 1);

        /// <summary>
        /// Sends the HDEL command to delete the specified hash field(s)
        /// </summary>
        /// <param name="keyName">The name of the Key for the entry in the database</param>
        /// <param name="token">A cancellation token to cancel the operation</param>
        /// <param name="fields">The name(s) of the hash field(s)</param>
        /// <returns>Returns the number of fields that were removed from the hash</returns>
        ValueTask<long> HDelAsync(string keyName, CancellationToken token, params string[] fields);

        /// <summary>
        /// Sends the EXISTS command to check if a/multiple key(s) exist
        /// </summary>
        /// <param name="token">A cancellation token to cancel the operation</param>
        /// <param name="keys">The name(s) of the Key(s) for the entry/entries in the database</param>
        /// <returns>Returns true if all the specified keys exist; otherwise, returns false</returns>
        ValueTask<bool> ExistsAsync(CancellationToken token, params string[] keys);

        /// <summary>
        /// Sends the EXPIREAT command to set a key expiration time
        /// </summary>
        /// <param name="keyName">The name of the Key for the entry in the database</param>
        /// <param name="when">The time when the key should be expired</param>
        /// <param name="token">A cancellation token to cancel the operation</param>
        /// <returns>Returns true if the expiration was set; otherwise, returns false</returns>
        ValueTask<bool> ExpireAtAsync(string keyName, DateTimeOffset when, CancellationToken token);

        /// <summary>
        /// Sends the DEL command to delete specified keys
        /// </summary>
        /// <param name="token">A cancellation token to cancel the operation</param>
        /// <param name="keys">The name(s) of the Key(s) for the entry/entries in the database</param>
        /// <returns>Returns the number of keys that were deleted</returns>
        ValueTask<long> DelAsync(CancellationToken token, params string[] keyNames);

        /// <summary>
        /// Starts subscribing to a pubsub channel
        /// </summary>
        /// <param name="channel">The name of the channel to subscribe to</param>
        /// <param name="handler">The callback channel messages are received on</param>
        /// <param name="token">A cancellation token to cancel the operation</param>
        ValueTask SubscribeAsync(string channel, ChannelWriter<object?> handler, CancellationToken token);

        /// <summary>
        /// Unsubscribes from a pubsub channel
        /// </summary>
        /// <param name="channel">The name of the channel to subscribe to</param>
        /// <param name="token">A cancellation token to cancel the operation</param>
        ValueTask UnsubscribeAsync(string channel, CancellationToken token);

        /// <summary>
        /// Sends a message to a pubsub channel
        /// </summary>
        /// <param name="channel">The name of the channel to subscribe to</param>
        /// <param name="message">The message to send to the channel</param>
        /// <param name="token">A cancellation token to cancel the operation</param>
        /// <returns>Returns the number of pubsub listeners the message was delivered to</returns>
        ValueTask<long> PublishAsync(string channel, string message, CancellationToken token);
    }
}
