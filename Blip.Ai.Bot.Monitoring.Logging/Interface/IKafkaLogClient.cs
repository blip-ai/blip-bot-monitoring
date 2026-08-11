namespace Blip.Ai.Bot.Monitoring.Logging.Interface
{
    public interface IKafkaLogClient
    {
        Task SendLogAsync(object logEntry, CancellationToken cancellationToken);
    }
}
