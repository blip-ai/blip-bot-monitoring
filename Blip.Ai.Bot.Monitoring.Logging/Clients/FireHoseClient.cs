using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Newtonsoft.Json;
using System.Text;

namespace Blip.Ai.Bot.Monitoring.Logging.Clients
{
    public class FireHoseClient : IFireHoseClient
    {
        private readonly FireHoseOptions _options;
        private readonly HttpClient? _httpClient;

        public FireHoseClient(FireHoseOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));

            _httpClient = new HttpClient
            {
                BaseAddress = new Uri(options.Address!)
            };

            _httpClient.DefaultRequestHeaders.Add("Authorization", options.Authentication);
        }

        public async Task SendLogToFireHoseAsync(object logEntry, CancellationToken cancellationToken = default)
        {
            var json = JsonConvert.SerializeObject(logEntry);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            var response = await _httpClient!.PostAsync(_options.Path, content, cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                throw new HttpRequestException($"FireHose logging failed with status: {response.StatusCode}");
            }
        }
    }
}
