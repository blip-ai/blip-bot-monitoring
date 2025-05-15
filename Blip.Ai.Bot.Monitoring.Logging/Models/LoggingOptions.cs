namespace Blip.Ai.Bot.Monitoring.Logging.Models;

public class LoggingOptions
{
    public SerilogOptions? Serilog { get; set; }
    public GrafanaOptions? Grafana { get; set; }
}