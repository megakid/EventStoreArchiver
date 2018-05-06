using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using DeadLinkCleaner.EventStore;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common.Log;
using EventStore.ClientAPI.Projections;
using Newtonsoft.Json;
using RestSharp;
using RestSharp.Authenticators;

namespace DeadLinkCleaner
{
    abstract class CheckpointUser
    {
        public string Name { get; protected set; }

        public abstract string CheckpointStream { get; }

        class PersistentSubscription : CheckpointUser
        {
            public override string CheckpointStream => $"$persistentsubscription-{{0}}::{Name}-checkpoint";

            public PersistentSubscription(string persistentSubscriptionName)
            {
                Name = persistentSubscriptionName;
            }

            public async Task<long?> GetCheckpointOnStream(IEventStoreConnection eventStoreConnection, string stream)
            {
                var checkpointStream = string.Format(CheckpointStream, stream);

                var headEvent = await eventStoreConnection.ReadEventAsync(checkpointStream, StreamPosition.End, false);

                if (headEvent.Status != EventReadStatus.Success)
                    // Safer to return zero -
                    // it is assumed that this PS is relevant on the stream given
                    // so in that case a checkpoint hasn't been written yet.
                    return 0;

                var resolvedEvent = headEvent.Event.Value;

                return long.Parse(Encoding.UTF8.GetString(resolvedEvent.Event.Data));
            }
        }

        class Projection : CheckpointUser
        {
            public override string CheckpointStream => $"$projections-{Name}-checkpoint";

            class ByCategory : Projection
            {
                public ByCategory()
                {
                    Name = "$by_category";
                }
            }

            class ByEventType : Projection
            {
                public ByEventType()
                {
                    Name = "$by_event_type";
                }
            }

            class StreamByCategory : Projection
            {
                public StreamByCategory()
                {
                    Name = "$stream_by_category";
                }
            }

            class Streams : Projection
            {
                public Streams()
                {
                    Name = "$streams";
                }
            }

            public async Task<long?> GetCheckpoint(IEventStoreConnection eventStoreConnection)
            {
                var checkpointStream = $"{Name}-checkpoint";
                var checkpointEvent =
                    await eventStoreConnection.ReadEventAsync(checkpointStream, StreamPosition.End, true);

                if (checkpointEvent.Status == EventReadStatus.Success)
                {
                }

                return 0;
            }
        }


        class CustomProjection : CheckpointUser
        {
            public override string CheckpointStream => $"$projections-{Name}-checkpoint";

            public CustomProjection(string projectionName)
            {
                Name = projectionName;
            }

            public async Task<long?> GetCheckpointOnStream(IEventStoreConnection eventStoreConnection, string stream)
            {
                var checkpointEvent =
                    await eventStoreConnection.ReadEventAsync(CheckpointStream, StreamPosition.End, false);

                if (checkpointEvent.Status != EventReadStatus.Success)
                {
                    // Here we assume if the projection hasn't written any checkpoints for this stream, 
                    // then the stream isn't relevant to the projection 
                    return null;
                }

                return checkpointEvent.Event?.Event.Metadata
            }
        }
    }

    public class PersistentSubscriptionCheckpointData
    {
        public long Checkpoint { get; set; }
    }

    public class CustomProjectionCheckpointMetadata
    {
        [JsonProperty("$v")] public string V { get; set; }

        [JsonProperty("$s")] public IDictionary<string, long> StreamCheckpoints { get; set; }
    }

    public class SystemProjectionCheckpointMetadata
    {
        [JsonProperty("$v")] public string V { get; set; }

        [JsonProperty("$c")] public long C { get; set; }

        [JsonProperty("$p")] public long P { get; set; }
    }

    public class TestPrepper : EventStoreUser
    {
        public TestPrepper(string connectionString) 
            : base(connectionString)
        {
        }

        public async Task Prep(string streamCategory, int streamId)
        {
            dynamic GetData() => Enumerable.Range(0, 5)
                .ToDictionary(j => $"Property{j}", j => (object) Guid.NewGuid());


            var events = 25000;
            
            var inserts = Enumerable.Range(0, events)
                .Batch(1000)
                .Select(batch =>
                    batch.Select(_ => new {eventId = Guid.NewGuid(), eventType = "MyType", data = GetData()}).ToArray())
                .Select()
                .ToArray();
            
            var es = await EventStore;

            var stream = $"{streamCategory}-{streamId}";
            es.AppendToStreamAsync(stream, ExpectedVersion.Any, )
        }
    }


    public abstract class EventStoreUser
    {
        protected AsyncLazy<IEventStoreConnection> EventStore { get; }
        protected AsyncLazy<IProjectionsManager> ProjectionsManager { get; }

        public EventStoreUser(string connectionString)
        {
            EventStore = new AsyncLazy<IEventStoreConnection>(() => EventStoreConnection.Create(connectionString));
            ProjectionsManager = new AsyncLazy<IProjectionsManager>(() => EventStoreProjectionsManager.Create(connectionString));
        }
    }

    public class EventStoreHttpDeadLinkCleanup : EventStoreUser
    {
        public EventStoreHttpDeadLinkCleanup(string connectionString)
            : base(connectionString)
        {
        }
        
        private async Task<long> FindFirstNonDeadLink2(string stream, long startAtInclusive)
        {
            IEnumerable<long> GetRange(long start, long count)
            {
                for (long i = start; i < start + count; i++)
                {
                    yield return i;
                }
            }

            const int ConcurrentCalls = 100;

            var esc = await _es;

            var slice = await esc.ReadStreamEventsForwardAsync(stream, startAtInclusive, 500, true);

            
            foreach (var re in slice.Events)
            {
                
            }
            
            long v = startAtInclusive;
            do
            {
                var tasks = GetRange(v, ConcurrentCalls)
                    .Select(async i =>
                    {
                        var response =
                            await _restClient.ExecuteGetTaskAsync<dynamic>(new RestRequest($"streams/{stream}/{i}"));
                        return (i, response);
                    });

                var responses = await Task.WhenAll(tasks);

                var firstLiveLink = responses
                    .Where(t => t.response.StatusCode == HttpStatusCode.OK)
                    .Select(t => (long?) t.i)
                    .FirstOrDefault();

                if (firstLiveLink.HasValue)
                    return firstLiveLink.Value;

                v += ConcurrentCalls;

                if (v > upToInclusive)
                    return upToInclusive;
            } while (true);
        }

        

        public async Task<long?> SafelyTruncateStream(string stream)
        {
            var pss = (await GetPersistentSubscriptions())
                .Where(ps => ps.Stream == stream)
                .ToArray();

            if (pss.Length > 0 && pss.Any(ps => !ps.Checkpoint.HasValue))
            {
                Console.WriteLine(
                    "One or more PersistentSubscriptions without a checkpoint location. Not truncating...");
                return 0;
            }

            // ReSharper disable once PossibleInvalidOperationException
            var minCheckpoint = pss.Length > 0 ? pss.Min(ps => ps.Checkpoint).Value : long.MaxValue;

            Console.WriteLine($"Lowest persistent subscription checkpoint on {stream} is {minCheckpoint}.");

            var currentTruncateBefore = (await GetTruncateBefore(stream)) ?? 0L;

            Console.WriteLine($"{stream} currently $tb = {currentTruncateBefore}.");

            var firstNonDeadLink = await FindFirstNonDeadLink(stream, currentTruncateBefore, minCheckpoint);

            Console.WriteLine($"First non-dead link on {stream} is {firstNonDeadLink}.");

            var safeVersionToTruncateBefore = new[] {minCheckpoint, firstNonDeadLink}.Min();

            var truncateBefore = safeVersionToTruncateBefore > 0 ? safeVersionToTruncateBefore : (long?) null;

            Console.WriteLine($"Setting stream metadata on {stream}, $tb = '{truncateBefore}'.");

            await SetTruncateBefore(stream, truncateBefore);

            return truncateBefore;
        }

        private async Task<long?> GetTruncateBefore(string stream)
        {
            var getMetadataReq = new RestRequest($"streams/{stream}/metadata", Method.GET);
            var getMetadataResponse = await _restClient.ExecuteGetTaskAsync<dynamic>(getMetadataReq);
            var metadata = getMetadataResponse.Data;

            var dict = (IDictionary<string, object>) metadata;
            if (dict.TryGetValue("$tb", out var output))
            {
                return Convert.ToInt64(output);
            }

            return null;
        }

        private async Task SetTruncateBefore(string stream, long? truncateBefore)
        {
            var getMetadataReq = new RestRequest($"streams/{stream}/metadata", Method.GET);
            var getMetadataResponse = await _restClient.ExecuteGetTaskAsync<dynamic>(getMetadataReq);
            var metadata = getMetadataResponse.Data;

            if (truncateBefore.HasValue)
                metadata["$tb"] = truncateBefore.Value;
            else
                ((IDictionary<string, object>) metadata).Remove("$tb");

            //https://groups.google.com/forum/#!topic/event-store/Y-QX6bdYYG8
            var setMetadataReq = new RestRequest($"streams/{stream}/metadata", Method.POST);
            setMetadataReq.AddJsonBody(metadata);
            setMetadataReq.AddHeader("ES-EventId", Guid.NewGuid().ToString());


            var setMetadataResponse = await _restClient.ExecutePostTaskAsync<dynamic>(setMetadataReq);

            // might already do this...
            if (!setMetadataResponse.IsSuccessful)
                throw setMetadataResponse.ErrorException;
        }

        private async Task<long> FindFirstNonDeadLink(string stream, long startAt, long upToInclusive)
        {
            IEnumerable<long> GetRange(long start, long count)
            {
                for (long i = start; i < start + count; i++)
                {
                    yield return i;
                }
            }

            if (upToInclusive < 0)
                throw new ArgumentOutOfRangeException(nameof(upToInclusive));

            const int ConcurrentCalls = 100;

            long v = startAt;
            do
            {
                var tasks = GetRange(v, ConcurrentCalls)
                    .Select(async i =>
                    {
                        var response =
                            await _restClient.ExecuteGetTaskAsync<dynamic>(new RestRequest($"streams/{stream}/{i}"));
                        return (i, response);
                    });

                var responses = await Task.WhenAll(tasks);

                var firstLiveLink = responses
                    .Where(t => t.response.StatusCode == HttpStatusCode.OK)
                    .Select(t => (long?) t.i)
                    .FirstOrDefault();

                if (firstLiveLink.HasValue)
                    return firstLiveLink.Value;

                v += ConcurrentCalls;

                if (v > upToInclusive)
                    return upToInclusive;
            } while (true);
        }

        private async Task<IReadOnlyList<PersistentSubscription>> GetPersistentSubscriptions()
        {
            var pss = new List<PersistentSubscription>();
            // First query subscriptions
            var result = await _restClient.ExecuteGetTaskAsync<dynamic>(new RestRequest("subscriptions"));

            foreach (var sub in result.Data)
            {
                string stream = sub["eventStreamId"];
                string groupName = sub["groupName"];
                string checkpointStreamUri =
                    ((string) sub["parkedMessageUri"]).Replace("parked", "checkpoint") + "/head/backward/1";

                long? checkpointLocation;
                try
                {
                    var checkpointContents =
                        await _restClient.ExecuteGetTaskAsync<dynamic>(new RestRequest(checkpointStreamUri));
                    var headEventUrl = checkpointContents.Data["entries"][0]["id"];

                    var checkpointData = await _restClient.ExecuteGetTaskAsync<dynamic>(new RestRequest(headEventUrl));
                    checkpointLocation = checkpointData.Data;
                }
                catch
                {
                    checkpointLocation = null;
                }

                Console.WriteLine(
                    $"Found PersistentSubscription on '{stream}' named '{groupName}' with checkpoint at '{checkpointLocation}'.");

                pss.Add(new PersistentSubscription(stream, groupName, checkpointLocation));
            }

            return pss;
        }

        /// <summary>
        /// Not used
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="events"></param>
        /// <returns></returns>
        public bool FillStream(string stream, int events)
        {
            dynamic GetData() => Enumerable.Range(0, 5)
                .ToDictionary(j => $"Property{j}", j => (object) Guid.NewGuid());

            var inserts = Enumerable.Range(0, events)
                .Batch(1000)
                .Select(batch =>
                    batch.Select(_ => new {eventId = Guid.NewGuid(), eventType = "MyType", data = GetData()}).ToArray())
                .ToArray();

            bool success = true;
            foreach (var insert in inserts)
            {
                var requestpost = new RestRequest($"streams/{stream}", Method.POST);
                requestpost.AddParameter(
                    "application/vnd.eventstore.events+json",
                    //requestpost.JsonSerializer.Serialize(new[] { new { eventId = Guid.NewGuid(), eventType = "$user-updated", data = metadata } }),
                    requestpost.JsonSerializer.Serialize(insert),
                    "application/vnd.eventstore.events+json",
                    ParameterType.RequestBody);

                success &= _restClient.Post<dynamic>(requestpost).IsSuccessful;
            }

            return success;
        }
    }
}