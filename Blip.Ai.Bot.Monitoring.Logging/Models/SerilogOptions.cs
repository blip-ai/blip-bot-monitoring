using System.Diagnostics.CodeAnalysis;

namespace Blip.Ai.Bot.Monitoring.Logging.Models;

[ExcludeFromCodeCoverage]
public class SerilogOptions
{
    public string Url { get; set; } = string.Empty;
    public string? ApiKey { get; set; }
    public string IndexFormat { get; set; } = "logs-{0:yyyy.MM.dd}";
    public string ApplicationName { get; set; } = "DefaultApp";
}
