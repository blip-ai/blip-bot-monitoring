using System.Text;
using System.Text.Json;
using Blip.Ai.Bot.Monitoring.Logging.Models;

namespace Blip.Ai.Bot.Monitoring.Logging.Provider
{
    public class TokenProvider(FireHoseOptions options, HttpClient httpClient)
    {
        private static readonly JsonSerializerOptions _jsonOptions = new() { PropertyNameCaseInsensitive = true };
        private TokenResponse? _token;
        private readonly SemaphoreSlim _lock = new(1, 1);
        private readonly FireHoseOptions _options = options;
        private readonly HttpClient _httpClient = httpClient;

        public async Task<string> GetAccessTokenAsync()
        {
            if (_token != null && _token.ExpiresAt > DateTime.UtcNow.AddMinutes(1))
                return _token.AccessToken;

            await _lock.WaitAsync();
            try
            {
                if (_token != null && _token.ExpiresAt > DateTime.UtcNow.AddMinutes(1))
                    return _token.AccessToken;

                if (
                    _token != null
                    && !string.IsNullOrEmpty(_token.RefreshToken)
                    && _token.RefreshExpiresAt > DateTime.UtcNow
                )
                {
                    try
                    {
                        _token = await RefreshTokenAsync(_token.RefreshToken);
                        return _token.AccessToken;
                    }
                    catch (Exception)
                    {
                        // Exception intentionally ignored.
                        // If refresh fails, the flow will continue and try to obtain a new token via login.
                    }
                }
                _token = await RequestNewTokenAsync();
                return _token.AccessToken;
            }
            finally
            {
                _lock.Release();
            }
        }

        private async Task<TokenResponse> RequestNewTokenAsync()
        {
            var payload = new { password = _options.Password, username = _options.UserName };
            return await GetTokenFromEndpointAsync(_options.UrlAuthentication!, payload);
        }

        private async Task<TokenResponse> RefreshTokenAsync(string refreshToken)
        {
            var payload = new { refresh_token = refreshToken };
            return await GetTokenFromEndpointAsync(_options.UrlRefreshToken!, payload);
        }

        private async Task<TokenResponse> GetTokenFromEndpointAsync(string url, object payload)
        {
            var json = JsonSerializer.Serialize(payload);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            HttpResponseMessage? response = null;

            try
            {
                response = await _httpClient.PostAsync(url, content);

                if (!response.IsSuccessStatusCode)
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    throw new InvalidOperationException(
                        $"Token endpoint returned HTTP {(int)response.StatusCode}: {errorContent}"
                    );
                }

                var body = await response.Content.ReadAsStringAsync();
                var token = JsonSerializer.Deserialize<TokenResponse>(
                    body,
                    _jsonOptions
                );

                if (token == null || string.IsNullOrEmpty(token.AccessToken))
                    throw new InvalidOperationException(
                        "Authentication response does not contain a valid access token."
                    );

                token.ExpiresAt = DateTime.UtcNow.AddSeconds(token.ExpiresIn);
                token.RefreshExpiresAt = DateTime.UtcNow.AddSeconds(token.RefreshExpiresIn);

                return token;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to obtain token from endpoint '{url}': {ex.Message}",
                    ex
                );
            }
            finally
            {
                response?.Dispose();
            }
        }
    }
}
