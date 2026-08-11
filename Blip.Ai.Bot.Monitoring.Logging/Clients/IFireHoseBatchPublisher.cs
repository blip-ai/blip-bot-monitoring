using Blip.Ai.Bot.Monitoring.Logging.Models;

namespace Blip.Ai.Bot.Monitoring.Logging.Clients;

internal interface IFireHoseBatchPublisher : IDisposable
{
    Task PublishAsync(FireHoseBatch batch, CancellationToken cancellationToken);
}