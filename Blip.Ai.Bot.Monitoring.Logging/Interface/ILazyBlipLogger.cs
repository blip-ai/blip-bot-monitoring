using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Models;

namespace Blip.Ai.Bot.Monitoring.Logging.Interface;

/// <summary>Optional best-effort API that defers snapshot allocation until after admission.</summary>
public interface ILazyBlipLogger : IBlipLogger
{
    /// <summary>
    /// Returns whether the event was accepted for publication, not whether Kafka delivered it.
    /// The synchronous factory must return an isolated snapshot and must not block.
    /// </summary>
    bool TryLogMessage(LogCategory category, string destination, Func<LogInput> inputFactory,
        Exception? exception = null);
}
