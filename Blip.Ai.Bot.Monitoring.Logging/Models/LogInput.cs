using System.Diagnostics.CodeAnalysis;

namespace Blip.Ai.Bot.Monitoring.Logging.Models;

[ExcludeFromCodeCoverage]
public class LogInput
{
    public string Title { get; set; } = "";
    public string IdMessage { get; set; } = "";
    public string From { get; set; } = "";
    public string To { get; set; } = "";
    public object? Data { get; set; }
    public string? Operation { get; set; }
}
