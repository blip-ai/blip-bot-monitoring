using Blip.Ai.Bot.Monitoring.Logging.Models;
using Newtonsoft.Json;
using Take.Elephant;

namespace Blip.Ai.Bot.Monitoring.Logging.Serialization;

internal sealed class FireHoseBatchSerializer : ISerializer<FireHoseBatch>
{
    public string Serialize(FireHoseBatch value) => JsonConvert.SerializeObject(value);

    public FireHoseBatch Deserialize(string value) =>
        JsonConvert.DeserializeObject<FireHoseBatch>(value) ?? new FireHoseBatch();
}