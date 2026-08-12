namespace Blip.Ai.Bot.Monitoring.Logging.Models;

public sealed class KafkaLogBatch
{
    public byte[][] Events { get; init; } = [];

    public DateTime Datetime { get; init; } = DateTime.UtcNow;
}
