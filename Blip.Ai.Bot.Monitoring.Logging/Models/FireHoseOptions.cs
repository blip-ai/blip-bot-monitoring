namespace Blip.Ai.Bot.Monitoring.Logging.Models
{
    /// <summary>
    /// Represents configuration options for connecting to a Firehose endpoint.
    /// </summary>
    public class FireHoseOptions
    {
        /// <summary>
        /// Gets or sets the Firehose's HTTP URL endpoint.
        /// </summary>
        public string? Address { get; set; }

        /// <summary>
        /// Gets or sets the username for Firehose authentication.
        /// </summary>
        public string? UserName { get; set; }

        /// <summary>
        /// Gets or sets the password for Firehose authentication.
        /// </summary>
        public string? Password { get; set; }

        /// <summary>
        /// Gets or sets the URL authentication string for the Firehose endpoint.
        /// </summary>
        public string? UrlAuthentication { get; set; }

        /// <summary>
        /// Gets or sets the URL used to refresh the authentication token for the Firehose endpoint.
        /// </summary>
        public string? UrlRefreshToken { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of log entries to hold in the in-memory channel buffer before applying backpressure.
        /// Defaults to 10000.
        /// </summary>
        public int ChannelCapacity { get; set; } = 10_000;

        /// <summary>
        /// Gets or sets the maximum number of entries to batch per HTTP request.
        /// Defaults to 100.
        /// </summary>
        public int BatchSize { get; set; } = 100;

        /// <summary>
        /// Gets or sets the maximum time to wait before flushing a partial batch, in milliseconds.
        /// Defaults to 500ms.
        /// </summary>
        public int FlushIntervalMs { get; set; } = 500;

        /// <summary>
        /// Determines whether the current FireHoseOptions instance has valid configuration.
        /// </summary>
        /// <returns>
        /// <c>true</c> if all required properties are set; otherwise, <c>false</c>.
        /// </returns>
        public bool IsValid()
        {
            return !string.IsNullOrEmpty(Address)
                && !string.IsNullOrEmpty(UserName)
                && !string.IsNullOrEmpty(Password)
                && !string.IsNullOrEmpty(UrlAuthentication);
        }
    }
}
