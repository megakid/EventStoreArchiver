using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using EventStore.ClientAPI;
using RestSharp;
using RestSharp.Authenticators;

namespace DeadLinkCleaner
{
    class PersistentSubscription
    {
        public PersistentSubscription(string stream, string @group, long? checkpoint)
        {
            Stream = stream;
            Group = @group;
            Checkpoint = checkpoint;
        }
        public string Stream { get; }
        public string Group { get; }
        public long? Checkpoint { get; }
    }

    public class EventStoreHttpDeadLinkCleanup
    {
        private RestClient _restClient;

        public EventStoreHttpDeadLinkCleanup(string baseUrl, string username, string password)
        {
            _restClient =
                new RestClient(baseUrl) { Authenticator = new HttpBasicAuthenticator(username, password) };
        }

        public async Task<long> SafelyTruncateStream(string stream)
        {

            var pss = (await GetPersistentSubscriptions())
                .Where(ps => ps.Stream == stream)
                .ToArray();

            if (pss.Length > 0 && pss.Any(ps => !ps.Checkpoint.HasValue))
            {
                Console.WriteLine("One or more PersistentSubscriptions without a checkpoint location.");
                return 0;
            }

            // ReSharper disable once PossibleInvalidOperationException
            var minCheckpoint = pss.Length > 0 ? pss.Min(ps => ps.Checkpoint).Value : long.MaxValue;
            var latestVersion = await FindFirstNonDeadLink(stream);

            var truncateBefore = new[] { minCheckpoint, latestVersion }.Min();

            if (truncateBefore > 0)
            {
                await SetTruncateBefore(stream, truncateBefore);
                //Console.WriteLine($"Success in setting $tb = {truncateBefore} metadata item on stream {stream}.");
            }

            return truncateBefore;
        }

        private async Task SetTruncateBefore(string stream, long? truncateBefore)
        {
            var getMetadataReq = new RestRequest($"streams/{stream}/metadata", Method.GET);
            var getMetadataResponse = await _restClient.ExecuteGetTaskAsync<dynamic>(getMetadataReq);
            var metadata = getMetadataResponse.Data;

            if (truncateBefore.HasValue)
                metadata["$tb"] = truncateBefore.Value;
            else
                ((IDictionary<string, object>)metadata).Remove("$tb");

            //https://groups.google.com/forum/#!topic/event-store/Y-QX6bdYYG8
            var setMetadataReq = new RestRequest($"streams/{stream}/metadata", Method.POST);
            setMetadataReq.AddJsonBody(metadata);
            setMetadataReq.AddHeader("ES-EventId", Guid.NewGuid().ToString());

            var setMetadataResponse = await _restClient.ExecutePostTaskAsync<dynamic>(setMetadataReq);

            // might already do this...
            if (!setMetadataResponse.IsSuccessful)
                throw setMetadataResponse.ErrorException;
        }

        private async Task<long> FindFirstNonDeadLink(string stream)
        {
            IEnumerable<long> GetRange(long start, int count)
            {
                for (long i = start; i < start + count; i++)
                {
                    yield return i;
                }
            }

            long v = 0;

            do
            {
                var tasks = GetRange(v, 100)
                    .Select(async i =>
                    {
                        var response = await _restClient.ExecuteGetTaskAsync<dynamic>(new RestRequest($"streams/{stream}/{i}"));
                        return (i, response);
                    });

                var responses = await Task.WhenAll(tasks);

                var firstLiveLink = responses.Where(t => t.response.StatusCode == HttpStatusCode.OK).Select(t => (long?)t.i).FirstOrDefault();
                if (firstLiveLink.HasValue)
                    return firstLiveLink.Value;

                v += 100;

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
                    ((string)sub["parkedMessageUri"]).Replace("parked", "checkpoint") + "/head/backward/1";

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

                pss.Add(new PersistentSubscription(stream, groupName, checkpointLocation));
            }

            return pss;
        }
    }

    public static class Extensions
    {
        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> collection, int batchSize)
        {
            List<T> nextbatch = new List<T>(batchSize);
            foreach (T item in collection)
            {
                nextbatch.Add(item);
                if (nextbatch.Count == batchSize)
                {
                    yield return nextbatch;
                    nextbatch = new List<T>();
                    // or nextbatch.Clear(); but see Servy's comment below
                }
            }

            if (nextbatch.Count > 0)
                yield return nextbatch;
        }
    }
    public class Options
    {
        [Option('i', "url", Required = true,
            HelpText = "EventStore HTTP URI string.")]
        public string Uri { get; set; }

        [Option('u', "user", HelpText = "Username")]
        public string Username { get; set; }
        [Option('p', "pass", HelpText = "Password")]
        public string Password { get; set; }

        [Option('s', "stream", Required = true,
            HelpText = "The stream to truncate")]
        public string Stream { get; set; }

        // Omitting long name, default --verbose
        [Option(
            HelpText = "Prints all messages to standard output.")]
        public bool Verbose { get; set; }

    }

    public class Program
    {
        public static int Main(string[] args)
        {
            var o = new Options();

            o.Uri = "http://localhost:2113";
            o.Stream = "$ce-AggregateCmds";
            o.Username = "admin";
            o.Password = "changeit";

            var p = new Program(o);

            var r = p.Go().GetAwaiter().GetResult();

            Console.WriteLine("Complete... press any key to exit;");
            Console.ReadKey();

            return r;
        }

        public Program(Options o)
        {
            _restClient =
                new RestClient(o.Uri) { Authenticator = new HttpBasicAuthenticator(o.Username, o.Password) };
            _stream = o.Stream;
        }

        public bool FillStream(string stream, int events)
        {
            dynamic GetData() => Enumerable.Range(0, 5)
                .ToDictionary(j => $"Property{j}", j => (object)Guid.NewGuid());

            var inserts = Enumerable.Range(0, events)
                .Batch(1000)
                .Select(batch => batch.Select(_ => new { eventId = Guid.NewGuid(), eventType = "MyType", data = GetData() }).ToArray())
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

        private int i = 0;
        private RestClient _restClient;
        private string _stream;



        public async Task<int> Go()
        {
            TrySetTruncateBefore("AggregateCmds-1", 4);

            FillStream("AggregateCmds-1", 50000);

            var pss = GetPersistentSubscriptions()
                .Where(ps => ps.Stream == _stream)
                .ToArray();

            if (pss.Any(ps => !ps.Checkpoint.HasValue))
            {
                // Can't do anything
                Console.WriteLine("One or more PersistentSubscriptions without a checkpoint location.");
                //return 0;
            }

            // ReSharper disable once PossibleInvalidOperationException
            var minCheckpoint = pss.Min(ps => ps.Checkpoint).Value;
            var latestVersion = FindFirstValidLinkVersion(_stream);

            var truncateBefore = new[] { minCheckpoint, latestVersion }.Min();

            if (truncateBefore > 0)
                TrySetTruncateBefore(_stream, truncateBefore);


            Console.WriteLine("Complete... press any key to exit;");
            Console.ReadKey();
            return 0;
        }

        private bool TrySetTruncateBefore(string stream, long? truncateBefore)
        {

            var request = new RestRequest($"streams/{stream}/metadata", Method.GET);

            var r = _restClient.Get<dynamic>(request);

            var metadata = r.Data;
            //request.AddJsonBody(new {truncateBefore = 0});
            //var r = restClient.Put(request);

            if (truncateBefore.HasValue)
                metadata["$tb"] = truncateBefore.Value;
            else
                ((IDictionary<string, object>)metadata).Remove("$tb");

            //https://groups.google.com/forum/#!topic/event-store/Y-QX6bdYYG8
            var requestpost = new RestRequest($"streams/{stream}/metadata", Method.POST);

            requestpost.AddParameter(requestpost.JsonSerializer.ContentType,
                //requestpost.JsonSerializer.Serialize(new[] { new { eventId = Guid.NewGuid(), eventType = "$user-updated", data = metadata } }),
                requestpost.JsonSerializer.Serialize(metadata),
                //"application/vnd.eventstore.events (+json/+xml)",
                ParameterType.RequestBody);

            //requestpost.AddHeader("ES-EventType", "$user-updated");
            requestpost.AddHeader("ES-EventId", Guid.NewGuid().ToString());
            var r2 = _restClient.Post<dynamic>(requestpost);

            //var result = restClient.Execute<dynamic>(new RestRequest($"streams/{o.Stream}"));
            return r2.IsSuccessful;
        }

        private long FindFirstValidLinkVersion(string stream)
        {
            bool linkDead = false;
            long v = -1;
            do
            {
                v++;
                var restRequest = new RestRequest($"streams/{stream}/{v}");
                //restRequest.AddHeader("ES-ResolveLinkTos", "true");
                linkDead = _restClient.Execute<dynamic>(restRequest).StatusCode == HttpStatusCode.NotFound;
            } while (linkDead);

            return v;
        }

        private IEnumerable<PersistentSubscription> GetPersistentSubscriptions()
        {
            // First query subscriptions
            var result = _restClient.Execute<dynamic>(new RestRequest("subscriptions"));
            foreach (var sub in result.Data)
            {
                string stream = sub["eventStreamId"];
                Console.WriteLine(stream);
                string groupName = sub["groupName"];
                Console.WriteLine(groupName);
                string checkpointStreamUri =
                    ((string)sub["parkedMessageUri"]).Replace("parked", "checkpoint") + "/head/backward/1";
                Console.WriteLine(checkpointStreamUri);

                long? checkpointLocation = null;
                try
                {
                    var headEventUrl =
                        _restClient.Execute<dynamic>(new RestRequest(checkpointStreamUri)).Data["entries"][0]["id"];
                    checkpointLocation = _restClient.Execute<dynamic>(new RestRequest(headEventUrl)).Data;
                    Console.WriteLine(checkpointLocation);
                }
                catch
                {
                }

                yield return new PersistentSubscription(stream, groupName, checkpointLocation);
            }
        }

        private async Task Do()
        {
            string connection = "";
            string stream = "";
            string group = "";

            // enumerate all streams
            IEventStoreConnection conn = EventStoreConnection.Create(connection);

            await Task.Run(() => conn.ConnectAsync());

            try
            {
                await conn.CreatePersistentSubscriptionAsync(stream, group,
                    PersistentSubscriptionSettings.Create(), null);
            }
            catch
            {
            }

            dynamic ps = conn.ConnectToPersistentSubscription(stream, group,
                (s, e) => Process(s, e),
                (s, r, ex) => HandleException(s, r, ex),
                null,
                2,
                false);

            Console.ReadKey();

        }

        private async Task Process(EventStorePersistentSubscriptionBase s, ResolvedEvent a)
        {
            Console.WriteLine(Encoding.UTF8.GetString(a.OriginalEvent.Data));

            s.Acknowledge(a);

            i++;
        }

        private static void HandleException(EventStorePersistentSubscriptionBase eventStorePersistentSubscriptionBase, SubscriptionDropReason subscriptionDropReason, Exception arg3)
        {


        }
    }
}
