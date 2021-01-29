using BaseCap.CloudAbstractions.Abstractions;
using BaseCap.CloudAbstractions.Implementations.Redis;
using Xunit;
using System.Collections.Generic;
using System;
using Microsoft.Extensions.Configuration;

namespace BaseCap.Tests.Unit.CloudAbstraction
{
    public class CloudAbstractionTest
    {
        private static readonly ICache _cache = ConnectToLocalCache();
        private static readonly string validTestKey = "cloud_abstraction_test";
        private static readonly string invalidTestKey = "cloud_abstraction_fail_test";

        public static ICache ConnectToLocalCache()
        {
            ICache cache = new RedisCache(new List<string>()
            {
                "localhost:6379"
            }, string.Empty, false);
            cache.SetupAsync();
            return cache;
        } 

        [Theory]
        [InlineData("fieldKey1", "fieldValue1")]
        [InlineData("fieldKey2", "")]
        [InlineData("", "fieldValue3")]
        public async void Cache_SetsGetsAndDeletes_WithCorrectData(string fieldKey, string fieldValue)
        {
            Assert.True(await _cache.SetHashFieldAsync(validTestKey, fieldKey, fieldValue, true));

            object? serializedGet = await _cache.GetHashFieldAsync(validTestKey, fieldKey).ConfigureAwait(false);
            string fieldValueAsString = serializedGet as string ?? string.Empty;
            Assert.Equal(fieldValue, fieldValueAsString);

            Assert.True(await _cache.DeleteCacheObjectAsync(validTestKey, true));
        }

        [Theory]
        [InlineData("testKey1", null, "fieldValue1")]
        [InlineData(null, "fieldKey2", "fieldValue2")]
        [InlineData(null, null, null)]
        public async void Cache_ThrowsArgumentException_WhenGivenNullValues(string? testKey, string? fieldKey, string? fieldValue)
        {
            // ignoring null arguments as we are testing for failure on null arguments
#pragma warning disable CS8604
            await Assert.ThrowsAsync<System.ArgumentException>(() => _cache.SetHashFieldAsync(testKey, fieldKey, fieldValue, true));
#pragma warning restore CS8604
        }

        [Fact]
        public async void Cache_ReturnsNull_WithIncorrectKey()
        {
            Assert.Null(await _cache.GetHashFieldAsync(invalidTestKey, string.Empty));
        }

        [Fact]
        public async void Cache_IncrementsFieldsByOne()
        {
            string fieldKey = "initialKey";
            long initialValue = 1;
            long maximumValue = 5;

            for (long i = 0; i < maximumValue; i++)
            {
                Assert.Equal(initialValue, await _cache.IncrementHashKeyAsync(validTestKey, fieldKey, true));
                initialValue++;
            }

            await _cache.DeleteCacheObjectAsync(validTestKey, true);
        }
    }
}
