using System.Diagnostics.Metrics;

namespace Blip.Ai.Bot.Monitoring.Logging.Models;

public enum MonitoringOutcome
{
    Disabled, Full, Filtered, Ineligible, EligibilityTimeout, SerializationError,
    Oversize, Shutdown, DeliveryFailed, Delivered, Expired, Admitted, Abandoned, Canceled
}

/// <summary>Per-instance atomic counters, also exported by the Blip.Monitoring meter.</summary>
public sealed class MonitoringDiagnostics
{
    private static readonly Meter Meter = new("Blip.Monitoring", "1.0");
    private static readonly Counter<long> Outcomes = Meter.CreateCounter<long>("monitoring.events");
    private static readonly UpDownCounter<long> Bytes = Meter.CreateUpDownCounter<long>("monitoring.retained.bytes");
    private static readonly UpDownCounter<long> Count = Meter.CreateUpDownCounter<long>("monitoring.retained.count");
    private static readonly UpDownCounter<long> Preparations = Meter.CreateUpDownCounter<long>("monitoring.preparations.active");
    private readonly long[] _counts = new long[Enum.GetValues<MonitoringOutcome>().Length];
    public long this[MonitoringOutcome outcome] => Interlocked.Read(ref _counts[(int)outcome]);
    internal void Record(MonitoringOutcome outcome, int count = 1)
    {
        Interlocked.Add(ref _counts[(int)outcome], count);
        Outcomes.Add(count, new KeyValuePair<string, object?>("outcome", outcome.ToString()));
    }
    internal static void Reserve(long bytes, int count)
    {
        Bytes.Add(bytes);
        Count.Add(count);
    }
    internal static void Preparation(int change) => Preparations.Add(change);
}
