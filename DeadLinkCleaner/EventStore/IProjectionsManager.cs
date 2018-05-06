using System.Collections.Generic;
using System.Threading.Tasks;
using EventStore.ClientAPI.Projections;
using EventStore.ClientAPI.SystemData;

namespace DeadLinkCleaner.EventStore
{
    public interface IProjectionsManager
    {
        Task EnableAsync(string name, UserCredentials userCredentials = null);
        Task DisableAsync(string name, UserCredentials userCredentials = null);
        Task AbortAsync(string name, UserCredentials userCredentials = null);
        Task CreateOneTimeAsync(string query, UserCredentials userCredentials = null);
        Task CreateTransientAsync(string name, string query, UserCredentials userCredentials = null);
        Task CreateContinuousAsync(string name, string query, UserCredentials userCredentials = null);
        Task CreateContinuousAsync(string name, string query, bool trackEmittedStreams, UserCredentials userCredentials = null);
        Task<string> ListAllAsStringAsync(UserCredentials userCredentials = null);
        Task<List<ProjectionDetails>> ListAllAsync(UserCredentials userCredentials = null);
        Task<string> ListOneTimeAsStringAsync(UserCredentials userCredentials = null);
        Task<List<ProjectionDetails>> ListOneTimeAsync(UserCredentials userCredentials = null);
        Task<string> ListContinuousAsStringAsync(UserCredentials userCredentials = null);
        Task<List<ProjectionDetails>> ListContinuousAsync(UserCredentials userCredentials = null);
        Task<string> GetStatusAsync(string name, UserCredentials userCredentials = null);
        Task<string> GetStateAsync(string name, UserCredentials userCredentials = null);
        Task<string> GetPartitionStateAsync(string name, string partitionId, UserCredentials userCredentials = null);
        Task<string> GetResultAsync(string name, UserCredentials userCredentials = null);
        Task<string> GetPartitionResultAsync(string name, string partitionId, UserCredentials userCredentials = null);
        Task<string> GetStatisticsAsync(string name, UserCredentials userCredentials = null);
        Task<string> GetQueryAsync(string name, UserCredentials userCredentials = null);
        Task UpdateQueryAsync(string name, string query, UserCredentials userCredentials = null);
        Task DeleteAsync(string name, UserCredentials userCredentials = null);
        Task DeleteAsync(string name, bool deleteEmittedStreams, UserCredentials userCredentials = null);
    }
}