using Blip.Ai.Bot.Monitoring.Logging.Models;
using Blip.Ai.Bot.Monitoring.Logging.Serialization;
using Confluent.Kafka;
using Take.Elephant.Kafka;

namespace Blip.Ai.Bot.Monitoring.Logging.Clients;

internal sealed class KafkaLogBatchPublisher : IKafkaLogBatchPublisher
{
    private readonly KafkaBatchSenderQueue<byte[]> _queue;

    public KafkaLogBatchPublisher(KafkaOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            LingerMs = options.ProducerLingerMilliseconds,
            BatchSize = options.ProducerBatchSize,
            EnableIdempotence = true,
            Acks = Acks.All,
            CompressionType = CompressionType.Zstd,
        };

        if (
            !string.IsNullOrWhiteSpace(options.SaslUsername)
            && !string.IsNullOrWhiteSpace(options.SaslPassword)
        )
        {
            producerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
            producerConfig.SaslMechanism = SaslMechanism.Plain;
            producerConfig.SaslUsername = options.SaslUsername;
            producerConfig.SaslPassword = options.SaslPassword;
        }

        _queue = new KafkaBatchSenderQueue<byte[]>(
            producerConfig,
            options.Topic!,
            new RawBytesSerializer()
        );
    }

    public Task PublishAsync(KafkaLogBatch batch, CancellationToken cancellationToken) =>
        _queue.EnqueueBatchAsync(batch.Events, cancellationToken);

    public void Dispose() => _queue.Dispose();
}
