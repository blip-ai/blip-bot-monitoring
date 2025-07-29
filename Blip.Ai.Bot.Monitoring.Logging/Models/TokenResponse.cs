using System.Text.Json.Serialization;

namespace Blip.Ai.Bot.Monitoring.Logging.Models
{
    public class TokenResponse
    {
        [JsonPropertyName("access_token")]
        public string AccessToken { get; set; } = string.Empty;

        [JsonPropertyName("expires_in")]
        public int ExpiresIn { get; set; }

        [JsonPropertyName("refresh_expires_in")]
        public int RefreshExpiresIn { get; set; }

        [JsonPropertyName("refresh_token")]
        public string RefreshToken { get; set; } = string.Empty;

        [JsonIgnore]
        public DateTime ExpiresAt { get; set; }

        [JsonIgnore]
        public DateTime RefreshExpiresAt { get; set; }
    }
}
