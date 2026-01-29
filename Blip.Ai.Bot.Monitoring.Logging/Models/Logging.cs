using System.Diagnostics.CodeAnalysis;
using Blip.Ai.Bot.Monitoring.Logging.Enums;

namespace Blip.Ai.Bot.Monitoring.Logging.Models;

[ExcludeFromCodeCoverage]
public class Logging
{
    public string FlowId { get; set; } = Guid.NewGuid().ToString();

    public string Tag { get; set; } = "BlipMonitoring";

    public string Cluster { get; set; } = string.Empty;

    public string TagSource { get; set; } = string.Empty;

    public LogCategory Category { get; set; }

    public string? Operation { get; set; }

    public string? EventType { get; set; }

    public string Title { get; set; } = string.Empty;

    public string IdMessage { get; set; } = string.Empty;

    public DateTime Datetime { get; set; } = DateTime.UtcNow;

    public string From { get; set; } = string.Empty;

    public string? OriginalFrom { get; set; } = string.Empty;

    public string To { get; set; } = string.Empty;

    public string? OriginalTo { get; set; } = string.Empty;

    public object? Data { get; set; }

    public string? Exception { get; set; }
}
