namespace Blip.Ai.Bot.Monitoring.Logging.Abstractions.Models;

public class LogInput
{
    public required string FlowId { get; set; }
    public required string Title { get; set; }
    public required string IdMessage { get; set; }
    public required string From { get; set; }
    public required string OriginalFrom { get; set; }
    public required string To { get; set; }
    public required string OriginalTo { get; set; }
    public required string Operation { get; set; }
    public required string EventType { get; set; }
    public required string StateId { get; set; }
    public required string Channel { get; set; }
    public required int FlowVersion { get; set; }
    public object? Data { get; set; }
    public object? SensitiveData { get; set; }
}
