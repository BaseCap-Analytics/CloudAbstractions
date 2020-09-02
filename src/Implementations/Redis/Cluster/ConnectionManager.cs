using BaseCap.CloudAbstractions.Redis.Database;
using BaseCap.CloudAbstractions.Redis.Database.Versions.V2;
using BaseCap.CloudAbstractions.Redis.Database.Versions.V3;
using System;
using System.IO.Pipelines;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Redis.Cluster
{
    public static class ConnectionManager
    {
        private static IPAddress? _connectTo;
        private static string _hostname = string.Empty;
        private static ushort _port;
        private static string? _password;
        private static byte[] _encryptionKey = Array.Empty<byte>();
        private static bool _useSsl;
        private static int _protocolVersion;

        public static async ValueTask InitializeAsync(
            string host,
            ushort port = 6379,
            string? password = null,
            bool useSsl = false,
            CancellationToken token = default(CancellationToken))
        {
            if (_connectTo != null)
            {
                throw new InvalidOperationException($"{nameof(ConnectionManager)} is already initialized");
            }

            if (IPAddress.TryParse(host, out _connectTo) == false)
            {
                IPAddress[] ips = await Dns.GetHostAddressesAsync(host);
                if (ips.Length == 0)
                {
                    throw new InvalidOperationException($"Could not resolve host '{host}'");
                }
                else
                {
                    _connectTo = ips[0];
                }
            }

            _hostname = host;
            _port = port;
            _password = password;
            _encryptionKey = Array.Empty<byte>();
            _useSsl = useSsl;
            _protocolVersion = await GetConnectionVersionAsync(token);
        }

        public static async ValueTask InitializeAsync(
            string host,
            byte[] encryptionKey,
            ushort port = 6379,
            string? password = null,
            bool useSsl = false,
            CancellationToken token = default(CancellationToken))
        {
            if (_connectTo != null)
            {
                throw new InvalidOperationException($"{nameof(ConnectionManager)} is already initialized");
            }

            if (IPAddress.TryParse(host, out _connectTo) == false)
            {
                IPAddress[] ips = await Dns.GetHostAddressesAsync(host);
                if (ips.Length == 0)
                {
                    throw new InvalidOperationException($"Could not resolve host '{host}'");
                }
                else
                {
                    _connectTo = ips[0];
                }
            }

            _hostname = host;
            _port = port;
            _password = password;
            _encryptionKey = encryptionKey;
            _useSsl = useSsl;
            _protocolVersion = await GetConnectionVersionAsync(token);
        }

        private static async ValueTask<int> GetConnectionVersionAsync(CancellationToken token)
        {
            if (await IsV3DatabaseAsync(token))
            {
                return 3;
            }
            else if (await IsV2DatabaseAsync(token))
            {
                return 2;
            }
            else
            {
                throw new InvalidOperationException("Unknown Redis Protocol version");
            }
        }

        private static async ValueTask<bool> IsDatabaseAsync<T>(T db, bool checkHello, CancellationToken token) where T : RedisDatabase
        {
            try
            {
                bool authed = true;
                if (string.IsNullOrWhiteSpace(_password) == false)
                {
                    authed = await db.AuthAsync(_password, token);
                }

                if (authed && checkHello)
                {
                    await db.HelloAsync(token);
                }

                return true;
            }
            catch
            {
                return false;
            }
        }

        private static async ValueTask<bool> IsV3DatabaseAsync(CancellationToken token)
        {
            try
            {
                bool result = false;
                Pipe pipe = new Pipe();
                using (PlaintextDataConnection connection = new PlaintextDataConnection(_hostname, _connectTo!, _port, _useSsl, pipe.Writer))
                {
                    await connection.OpenAsync(token);
                    using (V3Database db = new V3Database(connection, pipe.Reader))
                    {
                        result = await IsDatabaseAsync(db, checkHello: true, token);
                    }
                }

                return result;
            }
            catch
            {
                return false;
            }
        }

        private static async ValueTask<bool> IsV2DatabaseAsync(CancellationToken token)
        {
            try
            {
                bool result = false;
                Pipe pipe = new Pipe();
                using (PlaintextDataConnection connection = new PlaintextDataConnection(_hostname, _connectTo!, _port, _useSsl, pipe.Writer))
                {
                    await connection.OpenAsync(token);
                    using (V2Database db = new V2Database(connection, pipe.Reader))
                    {
                        result = await IsDatabaseAsync(db, checkHello: false, token);
                    }
                }

                return result;
            }
            catch
            {
                return false;
            }
        }

        public static async ValueTask<RedisDatabase> CreateDatabaseConnectionAsync(CancellationToken token)
        {
            if (_connectTo == null)
            {
                throw new InvalidOperationException($"{nameof(ConnectionManager)} must be initialized before connecting to a Redis Database");
            }

            Pipe pipe = new Pipe();
            ConnectionBase connection = _encryptionKey == Array.Empty<byte>() ?
                                            new PlaintextDataConnection(_hostname, _connectTo, _port, _useSsl, pipe.Writer) :
                                            new EncryptedDataConnection(_hostname, _connectTo, _port, _useSsl, pipe.Writer, _encryptionKey);
            await connection.OpenAsync(token);

            RedisDatabase db = _protocolVersion == 3 ? (RedisDatabase)new V3Database(connection, pipe.Reader) : new V2Database(connection, pipe.Reader);
            if (string.IsNullOrWhiteSpace(_password) == false)
            {
                await db.AuthAsync(_password, token);
            }

            return db;
        }
    }
}
