using System.Buffers;
using System.Text;
using System.Text.Json;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Take.Elephant;

namespace Blip.Ai.Bot.Monitoring.Logging.Serialization;

internal sealed class KafkaLogBatchSerializer : ISerializer<KafkaLogBatch>
{
    public string Serialize(KafkaLogBatch value)
    {
        var bufferWriter = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(bufferWriter);
        writer.WriteStartObject();
        writer.WritePropertyName("Events");
        writer.WriteStartArray();
        foreach (var ev in value.Events)
            writer.WriteRawValue(ev.AsSpan());
        writer.WriteEndArray();
        writer.WriteString("Datetime", value.Datetime);
        writer.WriteEndObject();
        writer.Flush();
        return Encoding.UTF8.GetString(bufferWriter.WrittenSpan);
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
                .Select(e => Encoding.UTF8.GetBytes(e.GetRawText()))
                .ToArray(),
            Datetime = doc.RootElement.GetProperty("Datetime").GetDateTime(),
        };
    }
}
