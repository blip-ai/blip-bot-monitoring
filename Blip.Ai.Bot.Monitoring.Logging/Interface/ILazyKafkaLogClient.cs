using Blip.Ai.Bot.Monitoring.Logging.Models;

namespace Blip.Ai.Bot.Monitoring.Logging.Interface;

/// <summary>Optional synchronous reservation API for allocating snapshots only after admission.</summary>
public interface ILazyKafkaLogClient : IKafkaLogClient
{
    /// <summary>
    /// Never retains the factory. A null result declines the reserved event (for example, filtering).
    /// Returns whether an immutable serialized event was queued, not whether it was delivered.
    /// </summary>
    bool TrySendLog(Func<KafkaLogPayload?> payloadFactory, CancellationToken cancellationToken = default);
}
