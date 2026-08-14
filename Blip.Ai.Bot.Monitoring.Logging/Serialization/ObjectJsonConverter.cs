using System.Text.Json;
using System.Text.Json.Serialization;
using Newtonsoft.Json.Linq;

namespace Blip.Ai.Bot.Monitoring.Logging.Serialization;

public sealed class ObjectJsonConverter : JsonConverter<object>
{
    private const int MaxDepth = 64;

    public override object? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        JsonSerializer.Deserialize<JsonElement>(ref reader, options);

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
    {
        if (value is JToken jToken)
        {
            WriteJTokenDirectly(writer, jToken, 0);
            return;
        }

        var runtimeType = value.GetType();
        if (runtimeType == typeof(object))
        {
            writer.WriteNullValue();
            return;
        }

        try
        {
            JsonSerializer.Serialize(writer, value, runtimeType, options);
        }
        catch (Exception ex) when (ex is NotSupportedException || ex is JsonException || ex is InvalidOperationException)
        {
            writer.WriteStringValue(value.ToString());
        }
    }

    private static void WriteJTokenDirectly(Utf8JsonWriter writer, JToken token, int depth)
    {
        if (depth >= MaxDepth)
        {
            writer.WriteNullValue();
            return;
        }

        switch (token.Type)
        {
            case JTokenType.Object:
                writer.WriteStartObject();
                foreach (var property in (JObject)token)
                {
                    writer.WritePropertyName(property.Key);
                    if (property.Value != null)
                    {
                        WriteJTokenDirectly(writer, property.Value, depth + 1);
                    }
                    else
                    {
                        writer.WriteNullValue();
                    }
                }
                writer.WriteEndObject();
                break;

            case JTokenType.Array:
                writer.WriteStartArray();
                foreach (var item in (JArray)token)
                {
                    if (item != null)
                    {
                        WriteJTokenDirectly(writer, item, depth + 1);
                    }
                    else
                    {
                        writer.WriteNullValue();
                    }
                }
                writer.WriteEndArray();
                break;

            case JTokenType.Integer:
                var rawInt = ((JValue)token).Value;
                if (rawInt is ulong u)
                    writer.WriteNumberValue(u);
                else
                    writer.WriteNumberValue(token.Value<long>());
                break;

            case JTokenType.Float:
                var d = token.Value<double>();
                if (double.IsNaN(d) || double.IsInfinity(d))
                    writer.WriteNullValue();
                else
                    writer.WriteNumberValue(d);
                break;

            case JTokenType.String:
                writer.WriteStringValue(token.Value<string>());
                break;

            case JTokenType.Boolean:
                writer.WriteBooleanValue(token.Value<bool>());
                break;

            case JTokenType.Null:
                writer.WriteNullValue();
                break;

          case JTokenType.Date:
                var rawValue = ((JValue)token).Value;
                if (rawValue is DateTimeOffset dto)
                {
                    writer.WriteStringValue(dto);
                }
                else
                {
                    writer.WriteStringValue(token.Value<DateTime>());
                }
                break;

            case JTokenType.Bytes:
                var bytes = token.Value<byte[]>();
                if (bytes is null)
                    writer.WriteNullValue();
                else
                    writer.WriteBase64StringValue(bytes);
                break;

            case JTokenType.Guid:
                writer.WriteStringValue(token.Value<Guid>());
                break;

            case JTokenType.TimeSpan:
                writer.WriteStringValue(token.Value<TimeSpan>().ToString("c"));
                break;

            default:
                writer.WriteStringValue(token.ToString());
                break;
        }
    }
}