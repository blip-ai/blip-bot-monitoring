using System.Reflection.Metadata;
using System.Runtime.Serialization;
using Lime.Messaging;
using Lime.Protocol;

namespace Blip.Ai.Bot.Monitoring.Logging.LocalValidator.Diagnostics;

[DataContract]
public class Trace : Lime.Protocol.Document
{
    public static readonly MediaType MediaType = MediaType.Parse("application/vnd.blip.trace+json");

    public Trace() : base(MediaType)
    {
        Timestamp = DateTimeOffset.UtcNow;
    }

    public Trace(MediaType mediaType) : base(mediaType)
    {
    }

    [DataMember(Name = "timestamp")]
    public DateTimeOffset Timestamp { get; set; }

    [DataMember(Name = "elapsedMilliseconds")]
    public long ElapsedMilliseconds { get; set; }

    [DataMember(Name = "warning")]
    public string? Warning { get; set; }

    [DataMember(Name = "error")]
    public string? Error { get; set; }
}
