namespace Blip.Ai.Bot.Monitoring.Logging.Models
{
    /// <summary>
    /// Represents configuration options for sending monitoring events to Kafka.
    /// </summary>
    public class KafkaOptions
    {
        /// <summary>
        /// Gets or sets the Kafka bootstrap servers used by the byte-native Kafka producer.
        /// </summary>
        public string? BootstrapServers { get; set; }

        /// <summary>
        /// Gets or sets the SASL username for authenticating with the Kafka broker (e.g., Confluent Cloud).
        /// </summary>
        public string? SaslUsername { get; set; }

        /// <summary>
        /// Gets or sets the SASL password for authenticating with the Kafka broker (e.g., Confluent Cloud).
        /// </summary>
        public string? SaslPassword { get; set; }

        /// <summary>
        /// Gets or sets the Kafka topic that receives monitoring batches.
        /// </summary>
        public string? Topic { get; set; }

        /// <summary>
        /// Gets or sets the maximum accumulated event payload bytes before flushing a batch.
        /// </summary>
        public int BatchMaxBytes { get; set; } = 1024 * 1024;

        /// <summary>
        /// Gets or sets the maximum serialized size, in bytes, allowed for a single log entry.
        /// Entries above this size are discarded before being sent, since the broker would otherwise
        /// reject them with "Message size too large" regardless of how many times they are retried.
        /// Should stay at or below the broker's message.max.bytes.
        /// </summary>
        public int MaxMessageBytes { get; set; } = 2097164;

        /// <summary>
        /// Gets or sets the maximum time to wait before flushing a non-empty batch.
        /// </summary>
        public int BatchMaxDelayMilliseconds { get; set; } = 1000;

        /// <summary>
        /// Gets or sets the total admitted item cap, including preparation and in-flight delivery.
        /// Saturated monitoring is dropped without waiting.
        /// </summary>
        public int QueueCapacity { get; set; } = 1024;

        /// <summary>Total reserved bytes, including serialization, queued and in-flight events.</summary>
        public long QueueMaxBytes { get; set; } = 16 * 1024 * 1024;

        public int MaxConcurrentSerializations { get; set; } = 2;
        public int BatchMaxCount { get; set; } = 128;
        public int ProducerQueueMaxMessages { get; set; } = 1024;
        public int ProducerQueueMaxKilobytes { get; set; } = 16 * 1024;
        public int DeliveryTimeoutMilliseconds { get; set; } = 5000;
        public int MaxEventAgeMilliseconds { get; set; } = 10000;

        /// <summary>
        /// Gets or sets the Kafka producer linger.ms value.
        /// </summary>
        public int ProducerLingerMilliseconds { get; set; } = 5;

        /// <summary>
        /// Gets or sets the Kafka producer batch.size value.
        /// </summary>
        public int ProducerBatchSize { get; set; } = 128 * 1024;

        /// <summary>
        /// Legacy compatibility property. Ignored: whole-batch replay can duplicate partial successes.
        /// </summary>
        public int PublishRetryCount { get; set; } = 0;

        /// <summary>
        /// Gets or sets the maximum time to wait while draining the queue during shutdown.
        /// </summary>
        public int ShutdownTimeoutMilliseconds { get; set; } = 5000;

        /// <summary>
        /// Legacy compatibility property. Ignored: admission never waits for queue space.
        /// </summary>
        public int WriteTimeoutMilliseconds { get; set; } = 5000;

        /// <summary>
        /// Determines whether the current KafkaOptions instance has valid configuration.
        /// </summary>
        /// <returns>
        /// <c>true</c> if all required properties are set; otherwise, <c>false</c>.
        /// </returns>
        public bool IsValid()
        {
            return !string.IsNullOrWhiteSpace(BootstrapServers)
                && !string.IsNullOrWhiteSpace(Topic)
                && !string.IsNullOrWhiteSpace(SaslUsername)
                && !string.IsNullOrWhiteSpace(SaslPassword)
                && BatchMaxBytes > 0
                && MaxMessageBytes > 0
                && MaxMessageBytes <= (int.MaxValue - 4096) / 6
                && BatchMaxDelayMilliseconds > 0
                && QueueCapacity > 0
                && QueueMaxBytes >= MaxMessageBytes
                && MaxConcurrentSerializations > 0
                && BatchMaxCount > 0
                && ProducerQueueMaxMessages > 0
                && ProducerQueueMaxKilobytes > 0
                && DeliveryTimeoutMilliseconds > ProducerLingerMilliseconds
                && MaxEventAgeMilliseconds > 0
                && ProducerLingerMilliseconds >= 0
                && ProducerBatchSize > 0
                && PublishRetryCount >= 0
                && ShutdownTimeoutMilliseconds > 0
                && WriteTimeoutMilliseconds > 0;
        }
    }
}
