using System;
using System.Net.Http;

namespace DeadLinkCleaner.EventStore.PersistentSubscriptions.Internals
{
    internal class ClientOperationState : IDisposable
    {
        public readonly HttpRequestMessage Request;
        public readonly Action<HttpResponse> OnSuccess;
        public readonly Action<Exception> OnError;

        public HttpResponse Response { get; set; }

        public ClientOperationState(HttpRequestMessage request, Action<HttpResponse> onSuccess, Action<Exception> onError)
        {
            Ensure.NotNull<HttpRequestMessage>(request, nameof (request));
            Ensure.NotNull<Action<HttpResponse>>(onSuccess, nameof (onSuccess));
            Ensure.NotNull<Action<Exception>>(onError, nameof (onError));
            this.Request = request;
            this.OnSuccess = onSuccess;
            this.OnError = onError;
        }

        public void Dispose()
        {
            this.Request.Dispose();
        }
    }
}