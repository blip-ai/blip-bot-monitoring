namespace Blip.Ai.Bot.Monitoring.Logging.Models
{
    /// <summary>
    /// Represents configuration options for sending monitoring events to Kafka.
    /// </summary>
    public class FireHoseOptions
    {
        /// <summary>
        /// Gets or sets the legacy Firehose HTTP URL endpoint.
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
        /// Gets or sets the Kafka bootstrap servers used by the Elephant Kafka sender.
        /// </summary>
        public string? BootstrapServers { get; set; }

        /// <summary>
        /// Gets or sets the Kafka topic that receives monitoring batches.
        /// </summary>
        public string? Topic { get; set; }

        /// <summary>
        /// Gets or sets the maximum accumulated event payload bytes before flushing a batch.
        /// </summary>
        public int BatchMaxBytes { get; set; } = 1024 * 1024;

        /// <summary>
        /// Gets or sets the maximum time to wait before flushing a non-empty batch.
        /// </summary>
        public int BatchMaxDelayMilliseconds { get; set; } = 1000;

        /// <summary>
        /// Gets or sets the in-memory queue capacity used to apply backpressure.
        /// </summary>
        public int QueueCapacity { get; set; } = 100000;

        /// <summary>
        /// Gets or sets the Kafka producer linger.ms value.
        /// </summary>
        public int ProducerLingerMilliseconds { get; set; } = 5;

        /// <summary>
        /// Gets or sets the Kafka producer batch.size value.
        /// </summary>
        public int ProducerBatchSize { get; set; } = 128 * 1024;

        /// <summary>
        /// Gets or sets how many times a failed batch publish is retried.
        /// </summary>
        public int PublishRetryCount { get; set; } = 3;

        /// <summary>
        /// Gets or sets the maximum time to wait while draining the queue during shutdown.
        /// </summary>
        public int ShutdownTimeoutMilliseconds { get; set; } = 30000;

        /// <summary>
        /// Determines whether the current FireHoseOptions instance has valid configuration.
        /// </summary>
        /// <returns>
        /// <c>true</c> if all required properties are set; otherwise, <c>false</c>.
        /// </returns>
        public bool IsValid()
        {
            return !string.IsNullOrWhiteSpace(BootstrapServers)
                && !string.IsNullOrWhiteSpace(Topic)
                && BatchMaxBytes > 0
                && BatchMaxDelayMilliseconds > 0
                && QueueCapacity > 0
                && ProducerLingerMilliseconds >= 0
                && ProducerBatchSize > 0
                && PublishRetryCount >= 0
                && ShutdownTimeoutMilliseconds > 0;
        }
    }
}
