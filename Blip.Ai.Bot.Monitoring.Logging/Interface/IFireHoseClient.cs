namespace Blip.Ai.Bot.Monitoring.Logging.Interface
{
    internal interface IFireHoseClient
    {
        Task SendLogToFireHoseAsync(object logEntry, CancellationToken cancellationToken);
    }
}
