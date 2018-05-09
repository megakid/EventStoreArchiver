using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading.Tasks;
using DeadLinkCleaner.EventStore.PersistentSubscriptions;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common.Log;
using EventStore.ClientAPI.Projections;
using EventStore.ClientAPI.SystemData;

namespace DeadLinkCleaner.EventStore.Projections
{
    public class EventStoreProjectionsManager : IProjectionsManager
    {
        public static async Task<IProjectionsManager> Create(string connectionString)
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

            return new EventStoreProjectionsManager(new ProjectionsManager(consoleLogger, new IPEndPoint(ip, uri.Port), TimeSpan.FromSeconds(10)));
        }

        private readonly ProjectionsManager _projectionsManager;

        public EventStoreProjectionsManager(ProjectionsManager projectionsManager)
        {
            _projectionsManager = projectionsManager;
        }

        public Task EnableAsync(string name, UserCredentials userCredentials = null)
        {
            return _projectionsManager.EnableAsync(name, userCredentials);
        }

        public Task DisableAsync(string name, UserCredentials userCredentials = null)
        {
            return _projectionsManager.DisableAsync(name, userCredentials);
        }

        public Task AbortAsync(string name, UserCredentials userCredentials = null)
        {
            return _projectionsManager.AbortAsync(name, userCredentials);
        }

        public Task CreateOneTimeAsync(string query, UserCredentials userCredentials = null)
        {
            return _projectionsManager.CreateOneTimeAsync(query, userCredentials);
        }

        public Task CreateTransientAsync(string name, string query, UserCredentials userCredentials = null)
        {
            return _projectionsManager.CreateTransientAsync(name, query, userCredentials);
        }

        public Task CreateContinuousAsync(string name, string query, UserCredentials userCredentials = null)
        {
            return _projectionsManager.CreateContinuousAsync(name, query, userCredentials);
        }

        public Task CreateContinuousAsync(string name, string query, bool trackEmittedStreams,
            UserCredentials userCredentials = null)
        {
            return _projectionsManager.CreateContinuousAsync(name, query, trackEmittedStreams, userCredentials);
        }

        public Task<string> ListAllAsStringAsync(UserCredentials userCredentials = null)
        {
            return _projectionsManager.ListAllAsStringAsync(userCredentials);
        }

        public Task<List<ProjectionDetails>> ListAllAsync(UserCredentials userCredentials = null)
        {
            return _projectionsManager.ListAllAsync(userCredentials);
        }

        public Task<string> ListOneTimeAsStringAsync(UserCredentials userCredentials = null)
        {
            return _projectionsManager.ListOneTimeAsStringAsync(userCredentials);
        }

        public Task<List<ProjectionDetails>> ListOneTimeAsync(UserCredentials userCredentials = null)
        {
            return _projectionsManager.ListOneTimeAsync(userCredentials);
        }

        public Task<string> ListContinuousAsStringAsync(UserCredentials userCredentials = null)
        {
            return _projectionsManager.ListContinuousAsStringAsync(userCredentials);
        }

        public Task<List<ProjectionDetails>> ListContinuousAsync(UserCredentials userCredentials = null)
        {
            return _projectionsManager.ListContinuousAsync(userCredentials);
        }

        public Task<string> GetStatusAsync(string name, UserCredentials userCredentials = null)
        {
            return _projectionsManager.GetStatusAsync(name, userCredentials);
        }

        public Task<string> GetStateAsync(string name, UserCredentials userCredentials = null)
        {
            return _projectionsManager.GetStateAsync(name, userCredentials);
        }

        public Task<string> GetPartitionStateAsync(string name, string partitionId,
            UserCredentials userCredentials = null)
        {
            return _projectionsManager.GetPartitionStateAsync(name, partitionId, userCredentials);
        }

        public Task<string> GetResultAsync(string name, UserCredentials userCredentials = null)
        {
            return _projectionsManager.GetResultAsync(name, userCredentials);
        }

        public Task<string> GetPartitionResultAsync(string name, string partitionId,
            UserCredentials userCredentials = null)
        {
            return _projectionsManager.GetPartitionResultAsync(name, partitionId, userCredentials);
        }

        public Task<string> GetStatisticsAsync(string name, UserCredentials userCredentials = null)
        {
            return _projectionsManager.GetStatisticsAsync(name, userCredentials);
        }

        public Task<string> GetQueryAsync(string name, UserCredentials userCredentials = null)
        {
            return _projectionsManager.GetQueryAsync(name, userCredentials);
        }

        public Task UpdateQueryAsync(string name, string query, UserCredentials userCredentials = null)
        {
            return _projectionsManager.UpdateQueryAsync(name, query, userCredentials);
        }

        public Task DeleteAsync(string name, UserCredentials userCredentials = null)
        {
            return _projectionsManager.DeleteAsync(name, userCredentials);
        }

        public Task DeleteAsync(string name, bool deleteEmittedStreams, UserCredentials userCredentials = null)
        {
            return _projectionsManager.DeleteAsync(name, deleteEmittedStreams, userCredentials);
        }
    }
}