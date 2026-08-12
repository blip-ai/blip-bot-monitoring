using System.Text;
using System.Text.Json;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Take.Elephant;

namespace Blip.Ai.Bot.Monitoring.Logging.Serialization;

internal sealed class KafkaLogBatchSerializer : ISerializer<KafkaLogBatch>
{
    public string Serialize(KafkaLogBatch value)
    {
        using var buffer = new MemoryStream();
        using var writer = new Utf8JsonWriter(buffer);
        writer.WriteStartObject();
        writer.WritePropertyName("Events");
        writer.WriteStartArray();
        foreach (var ev in value.Events)
            writer.WriteRawValue(ev);
        writer.WriteEndArray();
        writer.WriteString("Datetime", value.Datetime);
        writer.WriteEndObject();
        writer.Flush();
        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    public KafkaLogBatch Deserialize(string value)
    {
        using var doc = JsonDocument.Parse(value);
        if (doc.RootElement.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            throw new JsonException("Kafka log batch deserialized to null.");

        return new KafkaLogBatch
        {
            Events = doc.RootElement
                .GetProperty("Events")
                .EnumerateArray()
                .Select(e => e.GetRawText())
                .ToArray(),
            Datetime = doc.RootElement.GetProperty("Datetime").GetDateTime(),
        };
    }
}
