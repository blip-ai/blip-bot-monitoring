using System.Runtime.Serialization;

namespace Blip.Ai.Bot.Monitoring.Logging.LocalValidator.Diagnostics;

[DataContract]
public class OutputTrace : Trace
{
    [DataMember(Name = "stateId")]
    public string? StateId { get; set; }

    [DataMember(Name = "stateName")]
    public string? StateName { get; set; }

    [DataMember(Name = "input")]
    public string? Input { get; set; }

    [DataMember(Name = "conditionIndex")]
    public int? ConditionIndex { get; set; }
}
