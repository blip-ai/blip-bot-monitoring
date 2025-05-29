using System.Diagnostics.CodeAnalysis;

namespace Blip.Ai.Bot.Monitoring.Logging.Models;

[ExcludeFromCodeCoverage]
public class LoggingOptions
{
    public string? HostServiceName { get; set; }
    public SerilogOptions? Serilog { get; set; }
    public GrafanaOptions? Grafana { get; set; }
}
