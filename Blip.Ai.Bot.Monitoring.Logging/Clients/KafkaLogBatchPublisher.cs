using Blip.Ai.Bot.Monitoring.Logging.Models;
using Blip.Ai.Bot.Monitoring.Logging.Serialization;
using Confluent.Kafka;
using Take.Elephant.Kafka;

namespace Blip.Ai.Bot.Monitoring.Logging.Clients;

internal sealed class KafkaLogBatchPublisher : IKafkaLogBatchPublisher
{
    private readonly KafkaSenderQueue<KafkaLogBatch> _queue;

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

        _queue = new KafkaSenderQueue<KafkaLogBatch>(
            producerConfig,
            options.Topic!,
            new KafkaLogBatchSerializer()
        );
    }

    public Task PublishAsync(KafkaLogBatch batch, CancellationToken cancellationToken) =>
        _queue.EnqueueAsync(batch, cancellationToken);

    public void Dispose() => _queue.Dispose();
}