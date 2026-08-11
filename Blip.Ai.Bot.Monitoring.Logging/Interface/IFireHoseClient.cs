namespace Blip.Ai.Bot.Monitoring.Logging.Interface
{
    /// <summary>
    /// Defines the contract for sending log entries to the FireHose endpoint.
    /// </summary>
    public interface IFireHoseClient
    {
        /// <summary>
        /// Sends a single log entry to the FireHose endpoint asynchronously.
        /// </summary>
        Task SendLogToFireHoseAsync(object logEntry, CancellationToken cancellationToken);
    }
}
