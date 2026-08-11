using Blip.Ai.Bot.Monitoring.Logging.Models;
using Blip.Ai.Bot.Monitoring.Logging.Serialization;
using Confluent.Kafka;
using Take.Elephant.Kafka;

namespace Blip.Ai.Bot.Monitoring.Logging.Clients;

internal sealed class KafkaFireHoseBatchPublisher : IFireHoseBatchPublisher
{
    private readonly KafkaSenderQueue<FireHoseBatch> _queue;

    public KafkaFireHoseBatchPublisher(FireHoseOptions options)
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

        _queue = new KafkaSenderQueue<FireHoseBatch>(
            producerConfig,
            options.Topic!,
            new FireHoseBatchSerializer()
        );
    }

    public Task PublishAsync(FireHoseBatch batch, CancellationToken cancellationToken) =>
        _queue.EnqueueAsync(batch, cancellationToken);

    public void Dispose() => _queue.Dispose();
}