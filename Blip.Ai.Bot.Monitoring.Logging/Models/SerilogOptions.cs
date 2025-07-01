using System.Diagnostics.CodeAnalysis;

namespace Blip.Ai.Bot.Monitoring.Logging.Models;

[ExcludeFromCodeCoverage]
public class SerilogOptions
{
    public string? Url { get; set; }
    public string? ApiKey { get; set; }
    public string ApplicationName { get; set; } = "DefaultApp";
}
