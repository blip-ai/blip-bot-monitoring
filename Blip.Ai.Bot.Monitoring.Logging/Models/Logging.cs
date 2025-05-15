using Blip.Ai.Bot.Monitoring.Logging.Enums;

namespace Blip.Ai.Bot.Monitoring.Logging.Models;

public class Logging
{
    private static readonly string FlowInstanceId = Guid.NewGuid().ToString();

    public string FlowId { get; set; } = FlowInstanceId;
    public string Tag { get; set; } = "BlipMonitoring";
    public string TagSource { get; set; } = "";
    public LogCategory Category { get; set; }
    public string? Operation { get; set; }
    public string Title { get; set; } = "";
    public string IdMessage { get; set; } = "";
    public DateTime Datetime { get; set; } = DateTime.UtcNow;
    public string From { get; set; } = "";
    public string To { get; set; } = "";
    public object? Data { get; set; }
    public string? Ex { get; set; }
}
