using Blip.Ai.Bot.Monitoring.Logging.Models;
using Newtonsoft.Json;
using Take.Elephant;

namespace Blip.Ai.Bot.Monitoring.Logging.Serialization;

internal sealed class KafkaLogBatchSerializer : ISerializer<KafkaLogBatch>
{
    public string Serialize(KafkaLogBatch value) => JsonConvert.SerializeObject(value);

    public KafkaLogBatch Deserialize(string value) =>
        JsonConvert.DeserializeObject<KafkaLogBatch>(value)
        ?? throw new JsonSerializationException("Kafka log batch deserialized to null.");
}
