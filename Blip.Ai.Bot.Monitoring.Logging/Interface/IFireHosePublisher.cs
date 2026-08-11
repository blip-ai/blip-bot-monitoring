namespace Blip.Ai.Bot.Monitoring.Logging.Interface
{
    /// <summary>
    /// Defines a contract for publishing log entries to FireHose using a buffered channel.
    /// </summary>
    public interface IFireHosePublisher : IDisposable
    {
        /// <summary>
        /// Enqueues a log entry to be published asynchronously. Non-blocking.
        /// </summary>
        void Publish(object logEntry);
    }
}
