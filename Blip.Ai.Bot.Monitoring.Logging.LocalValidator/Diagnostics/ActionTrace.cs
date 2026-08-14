using Newtonsoft.Json.Linq;
using System.Runtime.Serialization;

namespace Blip.Ai.Bot.Monitoring.Logging.LocalValidator.Diagnostics;

[DataContract]
public class ActionTrace : Trace
{
    [DataMember(Name = "order")]
    public int Order { get; set; }

    [DataMember(Name = "type")]
    public string? Type { get; set; }

    [DataMember(Name = "parsedSettings")]
    public JRaw? ParsedSettings { get; set; }

    [DataMember(Name = "continueOnError")]
    public bool ContinueOnError { get; set; }

    [DataMember(Name = "actionId")]
    public string? ActionId { get; set; }

    [DataMember(Name = "actionTitle")]
    public string? ActionTitle { get; set; }
}
