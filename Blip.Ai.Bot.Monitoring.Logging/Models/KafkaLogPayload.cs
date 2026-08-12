using System.Diagnostics.CodeAnalysis;
using Blip.Ai.Bot.Monitoring.Logging.Abstractions.Models;
using Blip.Ai.Bot.Monitoring.Logging.Enums;

namespace Blip.Ai.Bot.Monitoring.Logging.Models;

[ExcludeFromCodeCoverage]
public sealed class KafkaLogPayload
{
    public string FlowId { get; init; } = string.Empty;
    public int? FlowVersion { get; init; }
    public string? Channel { get; init; }
    public string Tag { get; init; } = "BlipMonitoring";
    public string Cluster { get; init; } = string.Empty;
    public string TagSource { get; init; } = string.Empty;
    public LogCategory Category { get; init; }
    public string? Operation { get; init; }
    public string? EventType { get; init; }
    public string Title { get; init; } = string.Empty;
    public string IdMessage { get; init; } = string.Empty;
    public DateTime Datetime { get; init; } = DateTime.UtcNow;
    public string From { get; init; } = string.Empty;
    public string? OriginalFrom { get; init; }
    public string To { get; init; } = string.Empty;
    public string? OriginalTo { get; init; }
    public object? Data { get; init; }
    public object? SensitiveData { get; init; }
    public string? Exception { get; init; }
    public string? StateId { get; init; }

    public static KafkaLogPayload FromInput(
        LogInput input,
        LogCategory category,
        string cluster,
        Exception? exception = null,
        string tagSource = ""
    )
    {
        return new KafkaLogPayload
        {
            FlowId = input.FlowId,
            FlowVersion = input.FlowVersion,
            Channel = input.Channel,
            Category = category,
            Operation = input.Operation,
            EventType = input.EventType,
            Title = input.Title,
            IdMessage = input.IdMessage,
            From = input.From,
            OriginalFrom = input.OriginalFrom,
            To = input.To,
            OriginalTo = input.OriginalTo,
            Data = input.Data,
            SensitiveData = input.SensitiveData,
            StateId = input.StateId,
            Cluster = cluster,
            Exception = exception?.Message,
            TagSource = tagSource,
        };
    }
}
