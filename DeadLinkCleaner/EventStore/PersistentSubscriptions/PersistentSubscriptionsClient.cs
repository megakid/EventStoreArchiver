using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using DeadLinkCleaner.EventStore.PersistentSubscriptions.Internals;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using Newtonsoft.Json.Linq;
using HttpResponse = DeadLinkCleaner.EventStore.PersistentSubscriptions.Internals.HttpResponse;

namespace DeadLinkCleaner.EventStore.PersistentSubscriptions
{
    public sealed class PersistentSubscriptionDetails
    {
        /// <summary>
        /// Configuration of the persistent subscription.
        /// Only populated when retreived via <see cref="Event"/>
        /// </summary>
        public PersistentSubscriptionConfigDetails Config { get; set; }
        public List<PersistentSubscriptionConnectionDetails> Connections { get; set; }

        public string EventStreamId { get; set; }      
        public string GroupName { get; set; }
        public string Status { get; set; }
        public decimal AverageItemsPerSecond { get; set; }
        public string ParkedMessageUri { get; set; }
        public string GetMessagesUri { get; set; }
        public long TotalItemsProcessed { get; set; }
        public long CountSinceLastMeasurement { get; set; }
        public long LastProcessedEventNumber { get; set; }
        public long LastKnownEventNumber { get; set; }
        public int ReadBufferCount { get; set; }
        public long LiveBufferCount { get; set; }
        public int RetryBufferCount { get; set; }
        public int TotalInFlightMessages { get; set; }
    }

    public sealed class PersistentSubscriptionConfigDetails
    {
        public bool ResolveLinktos { get; set; }
        public long StartFrom { get; set; }
        public int MessageTimeoutMilliseconds { get; set; }
        public bool ExtraStatistics { get; set; }
        public int MaxRetryCount { get; set; }
        public int LiveBufferSize { get; set; }
        public int BufferSize { get; set; }
        public int ReadBatchSize { get; set; }
        public bool PreferRoundRobin { get; set; }
        public int CheckPointAfterMilliseconds { get; set; }
        public int MinCheckPointCount { get; set; }
        public int MaxCheckPointCount { get; set; }
        public int MaxSubscriberCount { get; set; }
        public string NamedConsumerStrategy { get; set; }
    }

    public sealed class PersistentSubscriptionConnectionDetails
    {
        public string From { get; set; }
        public string Username { get; set; }
        public int AverageItemsPerSecond { get; set; }
        public long TotalItems { get; set; }
        public int AvailableSlots { get; set; }
        public int InFlightMessages { get; set; }
    }

    internal class PersistentSubscriptionsClient
    {
        private readonly HttpAsyncClient _client;
        private readonly TimeSpan _operationTimeout;

        public PersistentSubscriptionsClient(ILogger log, TimeSpan operationTimeout)
        {
            this._operationTimeout = operationTimeout;
            this._client = new HttpAsyncClient(this._operationTimeout);
        }

        public Task<PersistentSubscriptionDetails> Describe(EndPoint endPoint, string stream, string subscriptionName,
            UserCredentials userCredentials = null, string httpSchema = EndpointExtensions.HTTP_SCHEMA)
        {
            return this.SendGet(endPoint.ToHttpUrl(httpSchema, "/subscriptions/{0}/{1}/info", stream, subscriptionName),
                    userCredentials, (int) HttpStatusCode.OK)
                .ContinueWith(x =>
                {
                    if (x.IsFaulted) throw x.Exception;
                    var r = JObject.Parse(x.Result);
                    return r != null ? r.ToObject<PersistentSubscriptionDetails>() : null;
                });
        }


        public Task<List<PersistentSubscriptionDetails>> List(EndPoint endPoint, string stream,
            UserCredentials userCredentials = null, string httpSchema = EndpointExtensions.HTTP_SCHEMA)
        {
            return SendGet(endPoint.ToHttpUrl(httpSchema, "/subscriptions/{0}", stream), userCredentials,
                    (int) HttpStatusCode.OK, (int) HttpStatusCode.NotFound)
                .ContinueWith(x =>
                {
                    if (x.IsFaulted) throw x.Exception;
                    var r = JArray.Parse(x.Result);
                    return r != null ? r.ToObject<List<PersistentSubscriptionDetails>>() : null;
                });
        }

        public Task<List<PersistentSubscriptionDetails>> List(EndPoint endPoint, UserCredentials userCredentials = null,
            string httpSchema = EndpointExtensions.HTTP_SCHEMA)
        {
            return SendGet(endPoint.ToHttpUrl(httpSchema, "/subscriptions"), userCredentials, (int) HttpStatusCode.OK, (int) HttpStatusCode.NotFound)
                .ContinueWith(x =>
                {
                    if (x.IsFaulted) throw x.Exception;
                    var r = JArray.Parse(x.Result);
                    return r != null ? r.ToObject<List<PersistentSubscriptionDetails>>() : null;
                });
        }

        public Task ReplayParkedMessages(EndPoint endPoint, string stream, string subscriptionName,
            UserCredentials userCredentials = null, string httpSchema = EndpointExtensions.HTTP_SCHEMA)
        {
            return this.SendPost(
                endPoint.ToHttpUrl(httpSchema, "/subscriptions/{0}/{1}/replayParked", stream, subscriptionName),
                string.Empty, userCredentials, (int) HttpStatusCode.OK);
        }


        private Task<string> SendGet(string url, UserCredentials userCredentials, params int[] expectedCodes)
        {
            TaskCompletionSource<string> source =
                new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            this._client.Get(url, userCredentials, response =>
            {
                if (expectedCodes.Contains(response.HttpStatusCode))
                    source.SetResult(response.Body);
                else
                    source.SetException(new PersistentSubscriptionCommandFailedException(
                        response.HttpStatusCode,
                        string.Format("Server returned {0} ({1}) for GET on {2}", response.HttpStatusCode,
                            response.StatusDescription, url)));
            }, new Action<Exception>(source.SetException), "");
            return source.Task;
        }


        private Task SendPost(string url, string content, UserCredentials userCredentials, int expectedCode)
        {
            TaskCompletionSource<object> source =
                new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            this._client.Post(url, content, "application/json", userCredentials, response =>
            {
                if (response.HttpStatusCode == expectedCode)
                    source.SetResult(null);
                else
                    source.SetException(new PersistentSubscriptionCommandFailedException(
                        response.HttpStatusCode,
                        string.Format("Server returned {0} ({1}) for POST on {2}", response.HttpStatusCode,
                            response.StatusDescription, url)));
            }, new Action<Exception>(source.SetException));
            return source.Task;
        }
        
    }
}