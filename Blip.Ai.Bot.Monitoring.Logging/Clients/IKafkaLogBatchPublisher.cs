using Blip.Ai.Bot.Monitoring.Logging.Models;

namespace Blip.Ai.Bot.Monitoring.Logging.Clients;

internal interface IKafkaLogBatchPublisher : IDisposable
{
    Task PublishAsync(KafkaLogBatch batch, CancellationToken cancellationToken);
}
