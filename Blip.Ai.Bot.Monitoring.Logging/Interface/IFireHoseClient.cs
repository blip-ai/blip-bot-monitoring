namespace Blip.Ai.Bot.Monitoring.Logging.Interface
{
    public interface IFireHoseClient
    {
        Task SendLogToFireHoseAsync(object logEntry, CancellationToken cancellationToken);
    }
}
