using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI.SystemData;

namespace DeadLinkCleaner.EventStore.PersistentSubscriptions.Internals
{
    internal class HttpAsyncClient
    {
        private static readonly UTF8Encoding UTF8NoBom = new UTF8Encoding(false);
        private HttpClient _client;

        public HttpAsyncClient(TimeSpan timeout)
        {
            this._client = new HttpClient();
            this._client.Timeout = timeout;
        }

        public void Get(string url, UserCredentials userCredentials, Action<HttpResponse> onSuccess, Action<Exception> onException, string hostHeader = "")
        {
            Ensure.NotNull<string>(url, nameof (url));
            Ensure.NotNull<Action<HttpResponse>>(onSuccess, nameof (onSuccess));
            Ensure.NotNull<Action<Exception>>(onException, nameof (onException));
            this.Receive("GET", url, userCredentials, onSuccess, onException, hostHeader);
        }

        public void Post(string url, string body, string contentType, UserCredentials userCredentials, Action<HttpResponse> onSuccess, Action<Exception> onException)
        {
            Ensure.NotNull<string>(url, nameof (url));
            Ensure.NotNull<string>(body, nameof (body));
            Ensure.NotNull<string>(contentType, nameof (contentType));
            Ensure.NotNull<Action<HttpResponse>>(onSuccess, nameof (onSuccess));
            Ensure.NotNull<Action<Exception>>(onException, nameof (onException));
            this.Send("POST", url, body, contentType, userCredentials, onSuccess, onException);
        }

        public void Delete(string url, UserCredentials userCredentials, Action<HttpResponse> onSuccess, Action<Exception> onException)
        {
            Ensure.NotNull<string>(url, nameof (url));
            Ensure.NotNull<Action<HttpResponse>>(onSuccess, nameof (onSuccess));
            Ensure.NotNull<Action<Exception>>(onException, nameof (onException));
            this.Receive("DELETE", url, userCredentials, onSuccess, onException, "");
        }

        public void Put(string url, string body, string contentType, UserCredentials userCredentials, Action<HttpResponse> onSuccess, Action<Exception> onException)
        {
            Ensure.NotNull<string>(url, nameof (url));
            Ensure.NotNull<string>(body, nameof (body));
            Ensure.NotNull<string>(contentType, nameof (contentType));
            Ensure.NotNull<Action<HttpResponse>>(onSuccess, nameof (onSuccess));
            Ensure.NotNull<Action<Exception>>(onException, nameof (onException));
            this.Send("PUT", url, body, contentType, userCredentials, onSuccess, onException);
        }

        private void Receive(string method, string url, UserCredentials userCredentials, Action<HttpResponse> onSuccess, Action<Exception> onException, string hostHeader = "")
        {
            HttpRequestMessage request = new HttpRequestMessage();
            request.RequestUri = new Uri(url);
            request.Method = new System.Net.Http.HttpMethod(method);
            if (userCredentials != null)
                this.AddAuthenticationHeader(request, userCredentials);
            if (!string.IsNullOrWhiteSpace(hostHeader))
                request.Headers.Host = hostHeader;
            ClientOperationState state = new ClientOperationState(request, onSuccess, onException);
            this._client.SendAsync(request).ContinueWith(this.RequestSent(state));
        }

        private void Send(string method, string url, string body, string contentType, UserCredentials userCredentials, Action<HttpResponse> onSuccess, Action<Exception> onException)
        {
            HttpRequestMessage request = new HttpRequestMessage();
            request.RequestUri = new Uri(url);
            request.Method = new System.Net.Http.HttpMethod(method);
            if (userCredentials != null)
                this.AddAuthenticationHeader(request, userCredentials);
            byte[] bytes = HttpAsyncClient.UTF8NoBom.GetBytes(body);
            StreamContent streamContent = new StreamContent((Stream) new MemoryStream(bytes));
            streamContent.Headers.ContentType = new MediaTypeHeaderValue(contentType);
            streamContent.Headers.ContentLength = new long?((long) bytes.Length);
            request.Content = (HttpContent) streamContent;
            ClientOperationState state = new ClientOperationState(request, onSuccess, onException);
            this._client.SendAsync(request).ContinueWith(this.RequestSent(state));
        }

        private void AddAuthenticationHeader(HttpRequestMessage request, UserCredentials userCredentials)
        {
            Ensure.NotNull<UserCredentials>(userCredentials, nameof (userCredentials));
            string s = string.Format("{0}:{1}", (object) userCredentials.Username, (object) userCredentials.Password);
            string base64String = Convert.ToBase64String(UTF8NoBom.GetBytes(s));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", base64String);
        }

        private Action<Task<HttpResponseMessage>> RequestSent(ClientOperationState state)
        {
            return (Action<Task<HttpResponseMessage>>) (task =>
            {
                try
                {
                    HttpResponseMessage result = task.Result;
                    state.Response = new HttpResponse(result);
                    result.Content.ReadAsStringAsync().ContinueWith(this.ResponseRead(state));
                }
                catch (Exception ex)
                {
                    state.Dispose();
                    state.OnError(ex);
                }
            });
        }

        private Action<Task<string>> ResponseRead(ClientOperationState state)
        {
            return (Action<Task<string>>) (task =>
            {
                try
                {
                    state.Response.Body = task.Result;
                    state.Dispose();
                    state.OnSuccess(state.Response);
                }
                catch (Exception ex)
                {
                    state.Dispose();
                    state.OnError(ex);
                }
            });
        }
    }
}