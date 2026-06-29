using System.Text;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Blip.Ai.Bot.Monitoring.Logging.Provider;
using Newtonsoft.Json;

namespace Blip.Ai.Bot.Monitoring.Logging.Clients
{
    public class FireHoseClient : IFireHoseClient
    {
        private readonly FireHoseOptions _options;
        private static TokenProvider? _tokenProvider;
        private static HttpClient? _httpClient;
        private static readonly object _initLock = new object();

        public FireHoseClient(FireHoseOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            EnsureInitialized(options);
        }

        private static void EnsureInitialized(FireHoseOptions options)
        {
            if (_tokenProvider != null && _httpClient != null)
                return;

            lock (_initLock)
            {
                if (_tokenProvider == null || _httpClient == null)
                {
                    _httpClient = new HttpClient();
                    _tokenProvider = new TokenProvider(options, _httpClient);
                }
            }
        }

        public async Task SendLogToFireHoseAsync(
            object logEntry,
            CancellationToken cancellationToken = default
        )
        {
            var accessToken = await _tokenProvider!.GetAccessTokenAsync();

            var json = JsonConvert.SerializeObject(logEntry);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            using var request = new HttpRequestMessage(HttpMethod.Post, _options.Address)
            {
                Content = content,
            };

            if (!request.Headers.TryAddWithoutValidation("Authorization", accessToken))
            {
                throw new InvalidOperationException(
                    "Failed to add Authorization header to FireHose request."
                );
            }

            using var response = await _httpClient!.SendAsync(request, cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                throw new HttpRequestException(
                    $"FireHose logging failed with status: {response.StatusCode}"
                );
            }
        }
    }
}
