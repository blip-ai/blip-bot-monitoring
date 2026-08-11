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
        /// Gets or sets the Kafka bootstrap servers (comma-separated host:port pairs).
        /// When null or empty, the Kafka publisher is disabled.
        /// </summary>
        public string? KafkaBootstrapServers { get; set; }

        /// <summary>
        /// Gets or sets the Kafka topic to which log entries are published.
        /// </summary>
        public string? KafkaTopic { get; set; }

        /// <summary>
        /// Gets or sets the total memory available for buffering unsent messages, in bytes.
        /// Defaults to 64 MB.
        /// </summary>
        public long BufferMemoryBytes { get; set; } = 67_108_864;

        /// <summary>
        /// Gets or sets the maximum size of a request batch sent to the broker, in bytes.
        /// Defaults to 16 KB.
        /// </summary>
        public int BatchSizeBytes { get; set; } = 16_384;

        /// <summary>
        /// Gets or sets the delay, in milliseconds, to wait for additional messages before sending a batch.
        /// Defaults to 5 ms.
        /// </summary>
        public double LingerMs { get; set; } = 5;

        /// <summary>
        /// Determines whether the current FireHoseOptions instance has valid configuration
        /// for at least one delivery mode (HTTP or Kafka).
        /// </summary>
        /// <returns>
        /// <c>true</c> if all required HTTP properties are set, or both Kafka properties are set;
        /// otherwise, <c>false</c>.
        /// </returns>
        public bool IsValid() => IsValidHttp() || IsValidKafka();

        private bool IsValidHttp() =>
            !string.IsNullOrEmpty(Address)
            && !string.IsNullOrEmpty(UserName)
            && !string.IsNullOrEmpty(Password)
            && !string.IsNullOrEmpty(UrlAuthentication);

        private bool IsValidKafka() =>
            !string.IsNullOrEmpty(KafkaBootstrapServers) && !string.IsNullOrEmpty(KafkaTopic);
    }
}
