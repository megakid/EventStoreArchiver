using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading.Tasks;
using DeadLinkCleaner.EventStore.PersistentSubscriptions.Internals;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common.Log;
using EventStore.ClientAPI.SystemData;

namespace DeadLinkCleaner.EventStore.PersistentSubscriptions
{
    public interface IPersistentSubscriptionsManager
    {
        Task<PersistentSubscriptionDetails> Describe(string stream, string subscriptionName, UserCredentials userCredentials = null);
        Task ReplayParkedMessages(string stream, string subscriptionName, UserCredentials userCredentials = null);

        
        Task<List<PersistentSubscriptionDetails>> List(string stream, UserCredentials userCredentials = null);
        Task<List<PersistentSubscriptionDetails>> List(UserCredentials userCredentials = null);
    }

    public class EventStorePersistentSubscriptionsManager : IPersistentSubscriptionsManager
    {
        public static async Task<IPersistentSubscriptionsManager> Create(string connectionString)
        {
            DbConnectionStringBuilder dbConnectionStringBuilder = new DbConnectionStringBuilder
            {
                ConnectionString = connectionString
            };
            if (!dbConnectionStringBuilder.ContainsKey("http"))
            {
                throw new ApplicationException("EventStore connection string doesn't contain an http URI.");
            }

            Uri uri = new Uri(dbConnectionStringBuilder["http"].ToString());

            ConnectionSettings connectionSettings = ConnectionString.GetConnectionSettings(connectionString);

            // The logger cannot be set in the connection string. We need to use some reflection to override it.
            var logfield = typeof(ConnectionSettings).GetField("Log", BindingFlags.Public | BindingFlags.Instance);
            if (logfield == null)
            {
                throw new ApplicationException("The ConnectionSettings class no longer has a Log field.");
            }

            var consoleLogger = new ConsoleLogger();

            logfield.SetValue(connectionSettings, consoleLogger);

            var ipAddresses = await Dns.GetHostAddressesAsync(uri.DnsSafeHost);

            IPAddress ip = ipAddresses.First(address => address.AddressFamily == AddressFamily.InterNetwork);

            return new EventStorePersistentSubscriptionsManager(consoleLogger, new IPEndPoint(ip, uri.Port), TimeSpan.FromSeconds(10));
        }

        private readonly PersistentSubscriptionsClient _client;
        private readonly EndPoint _httpEndPoint;
        private readonly string _httpSchema;

        public EventStorePersistentSubscriptionsManager(ILogger log, EndPoint httpEndPoint, TimeSpan operationTimeout,
            string httpSchema = EndpointExtensions.HTTP_SCHEMA)
        {
            Ensure.NotNull(log, nameof(log));
            Ensure.NotNull(httpEndPoint, nameof(httpEndPoint));
            this._client = new PersistentSubscriptionsClient(log, operationTimeout);
            this._httpEndPoint = httpEndPoint;
            this._httpSchema = httpSchema;
        }

        public Task<PersistentSubscriptionDetails> Describe(string stream, string subscriptionName,
            UserCredentials userCredentials = null)
        {
            Ensure.NotNullOrEmpty(stream, "stream");
            Ensure.NotNullOrEmpty(subscriptionName, "subscriptionName");
            return _client.Describe(_httpEndPoint, stream, subscriptionName, userCredentials, _httpSchema);
        }

        public Task ReplayParkedMessages(string stream, string subscriptionName, UserCredentials userCredentials = null)
        {
            Ensure.NotNullOrEmpty(stream, "stream");
            Ensure.NotNullOrEmpty(subscriptionName, "subscriptionName");
            return _client.ReplayParkedMessages(_httpEndPoint, stream, subscriptionName, userCredentials, _httpSchema);
        }

        public Task<List<PersistentSubscriptionDetails>> List(string stream,
            UserCredentials userCredentials = null)
        {
            Ensure.NotNullOrEmpty(stream, "stream");
            return _client.List(_httpEndPoint, stream, userCredentials, _httpSchema);
        }

        public Task<List<PersistentSubscriptionDetails>> List(UserCredentials userCredentials = null)
        {
            return _client.List(_httpEndPoint, userCredentials, _httpSchema);
        }
    }
}