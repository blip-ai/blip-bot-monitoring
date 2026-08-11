using System.Diagnostics.CodeAnalysis;

namespace Blip.Ai.Bot.Monitoring.Logging.Models;

[ExcludeFromCodeCoverage]
public class LoggingOptions
{
    public string? HostServiceName { get; set; }
    public bool IsEnabledMonitoring { get; set; } = true;
    public string? Cluster { get; set; }
    public SerilogOptions? Serilog { get; set; }
    public KafkaOptions? Kafka { get; set; }
}
