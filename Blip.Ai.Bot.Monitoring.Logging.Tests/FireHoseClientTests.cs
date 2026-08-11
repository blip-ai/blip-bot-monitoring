using System.Net;
using System.Reflection;
using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Blip.Ai.Bot.Monitoring.Logging.Provider;

namespace Blip.Ai.Bot.Monitoring.Logging.Tests
{
    public class FireHoseClientTests : IDisposable
    {
        private static readonly FireHoseOptions ValidOptions = new()
        {
            Address = "http://localhost:8080/firehose",
            UserName = "user",
            Password = "pass",
            UrlAuthentication = "http://localhost:8080/auth",
        };

        private readonly FakeHttpMessageHandler _fakeHandler;
        private readonly HttpClient _fakeHttpClient;

        public FireHoseClientTests()
        {
            _fakeHandler = new FakeHttpMessageHandler();
            _fakeHttpClient = new HttpClient(_fakeHandler);
            ResetStaticState();
        }

        public void Dispose()
        {
            ResetStaticState();
            _fakeHttpClient.Dispose();
            GC.SuppressFinalize(this);
        }

        private void InjectStaticDependencies()
        {
            var tokenProvider = new TokenProvider(ValidOptions, _fakeHttpClient);
            SetStaticField("_httpClient", _fakeHttpClient);
            SetStaticField("_tokenProvider", tokenProvider);
        }

        private static void ResetStaticState()
        {
            SetStaticField("_httpClient", null);
            SetStaticField("_tokenProvider", null);
        }

        private static void SetStaticField(string fieldName, object? value) =>
            typeof(FireHoseClient)
                .GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Static)!
                .SetValue(null, value);

        [Fact]
        public void Constructor_WithNullOptions_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new FireHoseClient(null!));
        }

        [Fact]
        public async Task SendLogToFireHoseAsync_WithSuccessResponse_ShouldNotThrow()
        {
            // Arrange
            HttpMethod? capturedMethod = null;
            string? capturedContentType = null;

            _fakeHandler.SetupAuthResponse(HttpStatusCode.OK, BuildTokenJson("valid-token"));
            _fakeHandler.SetupFireHoseResponse(
                HttpStatusCode.OK,
                onRequest: request =>
                {
                    capturedMethod = request.Method;
                    capturedContentType = request.Content?.Headers.ContentType?.MediaType;
                    return Task.CompletedTask;
                }
            );

            InjectStaticDependencies();
            var client = new FireHoseClient(ValidOptions);

            // Act
            await client.SendLogToFireHoseAsync(new { Message = "test" });

            // Assert
            Assert.Equal(HttpMethod.Post, capturedMethod);
            Assert.Equal("application/json", capturedContentType);
        }

        [Fact]
        public async Task SendLogToFireHoseAsync_ShouldSetAuthorizationHeaderFromToken()
        {
            // Arrange
            const string expectedToken = "test-access-token";
            string? capturedAuthHeader = null;

            _fakeHandler.SetupAuthResponse(HttpStatusCode.OK, BuildTokenJson(expectedToken));
            _fakeHandler.SetupFireHoseResponse(
                HttpStatusCode.OK,
                onRequest: request =>
                {
                    request.Headers.TryGetValues("Authorization", out var values);
                    capturedAuthHeader = values?.FirstOrDefault();
                    return Task.CompletedTask;
                }
            );

            InjectStaticDependencies();
            var client = new FireHoseClient(ValidOptions);

            // Act
            await client.SendLogToFireHoseAsync(new { Message = "test" });

            // Assert - TryAddWithoutValidation("Authorization", token) must have returned true
            Assert.Equal(expectedToken, capturedAuthHeader);
        }

        [Fact]
        public async Task SendLogToFireHoseAsync_WithNonSuccessStatusCode_ShouldThrowHttpRequestException()
        {
            // Arrange
            _fakeHandler.SetupAuthResponse(HttpStatusCode.OK, BuildTokenJson("valid-token"));
            _fakeHandler.SetupFireHoseResponse(HttpStatusCode.InternalServerError);

            InjectStaticDependencies();
            var client = new FireHoseClient(ValidOptions);

            // Act
            var exception = await Assert.ThrowsAsync<HttpRequestException>(() =>
                client.SendLogToFireHoseAsync(new { Message = "test" })
            );

            // Assert
            Assert.Contains("InternalServerError", exception.Message);
        }

        private static string BuildTokenJson(string accessToken) =>
            "{\"access_token\":\""
            + accessToken
            + "\",\"expires_in\":3600,\"refresh_expires_in\":7200,\"refresh_token\":\"refresh\"}";

        private sealed class FakeHttpMessageHandler : HttpMessageHandler
        {
            private readonly Dictionary<
                string,
                Func<HttpRequestMessage, Task<HttpResponseMessage>>
            > _handlers = new();

            public void SetupAuthResponse(HttpStatusCode statusCode, string content) =>
                _handlers[ValidOptions.UrlAuthentication!] = _ =>
                    Task.FromResult(
                        new HttpResponseMessage(statusCode) { Content = new StringContent(content) }
                    );

            public void SetupFireHoseResponse(
                HttpStatusCode statusCode,
                Func<HttpRequestMessage, Task>? onRequest = null
            ) =>
                _handlers[ValidOptions.Address!] = async request =>
                {
                    if (onRequest != null)
                        await onRequest(request);
                    return new HttpResponseMessage(statusCode)
                    {
                        Content = new StringContent(string.Empty),
                    };
                };

            protected override Task<HttpResponseMessage> SendAsync(
                HttpRequestMessage request,
                CancellationToken cancellationToken
            )
            {
                var url = request.RequestUri?.ToString() ?? string.Empty;
                return _handlers.TryGetValue(url, out var handler)
                    ? handler(request)
                    : Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound));
            }
        }
    }
}
