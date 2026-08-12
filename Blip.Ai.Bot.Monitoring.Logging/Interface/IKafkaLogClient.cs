using Blip.Ai.Bot.Monitoring.Logging.Models;

namespace Blip.Ai.Bot.Monitoring.Logging.Interface
{
    public interface IKafkaLogClient
    {
        Task SendLogAsync(KafkaLogPayload logEntry, CancellationToken cancellationToken);
    }
}
