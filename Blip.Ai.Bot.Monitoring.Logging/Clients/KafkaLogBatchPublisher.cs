using Blip.Ai.Bot.Monitoring.Logging.Models;
using Confluent.Kafka;

namespace Blip.Ai.Bot.Monitoring.Logging.Clients;

internal sealed class KafkaLogBatchPublisher : IKafkaLogBatchPublisher
{
    private readonly IProducer<Null, byte[]> _producer;
    private readonly string _topic;
    private readonly int _maximumBatchCount;

    public KafkaLogBatchPublisher(KafkaOptions options)
        : this(options, new ProducerBuilder<Null, byte[]>(CreateConfig(options))
            .SetValueSerializer(Serializers.ByteArray).Build()) { }

    internal KafkaLogBatchPublisher(KafkaOptions options, IProducer<Null, byte[]> producer)
    {
        _producer = producer;
        _topic = options.Topic!;
        _maximumBatchCount = options.BatchMaxCount;
    }

    internal static ProducerConfig CreateConfig(KafkaOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!options.IsValid()) throw new ArgumentException("Kafka options are invalid.", nameof(options));
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            LingerMs = options.ProducerLingerMilliseconds, BatchSize = options.ProducerBatchSize,
            EnableIdempotence = false, Acks = Acks.Leader, CompressionType = CompressionType.Zstd,
            QueueBufferingMaxMessages = options.ProducerQueueMaxMessages,
            QueueBufferingMaxKbytes = options.ProducerQueueMaxKilobytes,
            MessageTimeoutMs = options.DeliveryTimeoutMilliseconds,
            RequestTimeoutMs = options.DeliveryTimeoutMilliseconds,
            SocketTimeoutMs = options.DeliveryTimeoutMilliseconds,
            MessageMaxBytes = Math.Max(1000, options.MaxMessageBytes + 1024),
            EnableGaplessGuarantee = false,
        };
        if (!string.IsNullOrWhiteSpace(options.SaslUsername) && !string.IsNullOrWhiteSpace(options.SaslPassword))
        {
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.Plain;
            config.SaslUsername = options.SaslUsername;
            config.SaslPassword = options.SaslPassword;
        }
        return config;
    }

    public async Task PublishAsync(KafkaLogBatch batch, CancellationToken cancellationToken)
    {
        if (batch.Events.Length > _maximumBatchCount)
            throw new ArgumentException("Batch exceeds the configured count budget.", nameof(batch));
        cancellationToken.ThrowIfCancellationRequested();
        var deliveries = new List<Task<DeliveryResult<Null, byte[]>>>(batch.Events.Length);
        try
        {
            foreach (var bytes in batch.Events)
            {
                // This fan-out is bounded by BatchMaxCount. Native delivery reports, not managed
                // cancellation, determine when the producer releases ownership of these bytes.
                deliveries.Add(_producer.ProduceAsync(_topic, new Message<Null, byte[]> { Value = bytes }));
            }
            await Task.WhenAll(deliveries).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // A synchronous queue-full error can follow successful Produce calls. Observe all
            // already submitted records before releasing the batch; never replay their successes.
            try { await Task.WhenAll(deliveries).ConfigureAwait(false); }
            catch (Exception) { }
            throw new PartialBatchException(deliveries.Count(task => task.IsCompletedSuccessfully));
        }
    }

    public void Dispose() => _producer.Dispose();
}

internal sealed class PartialBatchException(int delivered) : Exception
{
    public int Delivered { get; } = delivered;
}
