using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace Blip.Ai.Bot.Monitoring.Logging.Clients
{
    /// <summary>
    /// A Kafka-backed publisher that serialises log entries to JSON and produces
    /// them to the configured topic using the native Confluent.Kafka producer.
    /// Buffering, batching, and backpressure are handled entirely by the producer
    /// client, avoiding the overhead of a custom channel-based implementation.
    /// </summary>
    public sealed class KafkaFireHosePublisher : IFireHosePublisher
    {
        private readonly IProducer<Null, string> _producer;
        private readonly string _topic;

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaFireHosePublisher"/> using
        /// the supplied options to configure the Kafka producer.
        /// </summary>
        /// <param name="options">FireHose options that contain Kafka bootstrap and buffer settings.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="options"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">
        /// Thrown when <see cref="FireHoseOptions.KafkaBootstrapServers"/> or
        /// <see cref="FireHoseOptions.KafkaTopic"/> is null or empty.
        /// </exception>
        public KafkaFireHosePublisher(FireHoseOptions options)
            : this(options, null) { }

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaFireHosePublisher"/> with an
        /// externally supplied producer. Intended for unit-testing.
        /// </summary>
        /// <param name="options">FireHose options that contain the topic name.</param>
        /// <param name="producer">
        /// A pre-built Kafka producer. When <c>null</c>, a producer is created from
        /// the settings in <paramref name="options"/>.
        /// </param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="options"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">
        /// Thrown when <see cref="FireHoseOptions.KafkaTopic"/> is null or empty and
        /// no producer is injected.
        /// </exception>
        internal KafkaFireHosePublisher(FireHoseOptions options, IProducer<Null, string>? producer)
        {
            if (options is null)
                throw new ArgumentNullException(nameof(options));

            if (string.IsNullOrEmpty(options.KafkaTopic))
                throw new ArgumentException(
                    "KafkaTopic must not be null or empty.",
                    nameof(options)
                );

            _topic = options.KafkaTopic;

            if (producer is not null)
            {
                _producer = producer;
            }
            else
            {
                if (string.IsNullOrEmpty(options.KafkaBootstrapServers))
                    throw new ArgumentException(
                        "KafkaBootstrapServers must not be null or empty.",
                        nameof(options)
                    );

                var config = new ProducerConfig
                {
                    BootstrapServers = options.KafkaBootstrapServers,
                    // BufferMemoryBytes is in bytes; QueueBufferingMaxKbytes expects KB
                    QueueBufferingMaxKbytes = (int)(options.BufferMemoryBytes / 1024),
                    BatchSize = options.BatchSizeBytes,
                    LingerMs = options.LingerMs,
                    MessageMaxBytes = 1_000_000,
                };

                _producer = new ProducerBuilder<Null, string>(config).Build();
            }
        }

        /// <inheritdoc />
        public void Publish(object logEntry)
        {
            if (logEntry is null)
                return;

            var json = JsonConvert.SerializeObject(logEntry);
            var message = new Message<Null, string> { Value = json };

            try
            {
                _producer.Produce(_topic, message);
            }
            catch (ProduceException<Null, string>)
            {
                // Swallow to prevent crashing the caller — mirroring the previous implementation.
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            try
            {
                _producer.Flush(TimeSpan.FromSeconds(5));
            }
            catch
            {
                // Best-effort flush; do not propagate errors during shutdown.
            }

            _producer.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
