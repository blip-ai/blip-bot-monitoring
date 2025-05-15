using Serilog.Events;

namespace Blip.Ai.Bot.Monitoring.Logging.Models;

public class GrafanaOptions
{
    public string LokiUri { get; set; }
    public string DashboardUid { get; set; }
    public string Description { get; set; }
    public string? LokiHeaderName { get; set; }
    public string? LokiHeaderValue { get; set; }
    public string? LokiLabels { get; set; }
    public IEnumerable<string>? LokiPropertiesAsLabels { get; set; }
    public LogEventLevel LogLevel { get; set; }
    public string? LokiLogin { get; set; }
    public string LokiPassword { get; set; }
}
