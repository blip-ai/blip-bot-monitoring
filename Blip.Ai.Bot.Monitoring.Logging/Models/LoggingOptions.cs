using System.Diagnostics.CodeAnalysis;

namespace Blip.Ai.Bot.Monitoring.Logging.Models;

[ExcludeFromCodeCoverage]
public class LoggingOptions
{
    public SerilogOptions? Serilog { get; set; }
    public GrafanaOptions? Grafana { get; set; }
}
