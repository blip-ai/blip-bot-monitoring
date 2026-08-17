using System.Runtime.Serialization;
using Newtonsoft.Json.Linq;

namespace Blip.Ai.Bot.Monitoring.Logging.LocalValidator.Diagnostics;

[DataContract]
public class StateTrace : Trace
{
    public StateTrace()
    {
        Id = Guid.NewGuid().ToString();
        InputActions = new List<ActionTrace>();
        OutputActions = new List<ActionTrace>();
        Outputs = new List<OutputTrace>();
        AfterStateChangedActions = new List<ActionTrace>();
        LocalCustomActions = new List<ActionTrace>();
    }

    [DataMember(Name = "id")]
    public string Id { get; set; }

    [DataMember(Name = "stateId")]
    public string? StateId { get; set; }

    [DataMember(Name = "stateName")]
    public string? StateName { get; set; }

    [DataMember(Name = "input")]
    public string? Input { get; set; }

    [DataMember(Name = "inputActions")]
    public ICollection<ActionTrace> InputActions { get; set; }

    [DataMember(Name = "outputActions")]
    public ICollection<ActionTrace> OutputActions { get; set; }

    [DataMember(Name = "outputs")]
    public ICollection<OutputTrace> Outputs { get; set; }

    [DataMember(Name = "extensionData")]
    public IDictionary<string, JToken>? ExtensionData { get; set; }

    [DataMember(Name = "afterStateChangedActions")]
    public ICollection<ActionTrace> AfterStateChangedActions { get; set; }

    [DataMember(Name = "localCustomActions")]
    public ICollection<ActionTrace> LocalCustomActions { get; set; }
}
