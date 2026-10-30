using System.Text.Json;
using System.Text.Json.Serialization;
using Newtonsoft.Json.Linq;
using Serilog;

namespace Blip.Ai.Bot.Monitoring.Logging.Serialization;

public class ObjectJsonConverter(
    ILogger? logger,
    bool isEnabledLoggingJsonParseErrors,
    int maximumRawCharacters
) : JsonConverter<object>
{
    public ObjectJsonConverter(ILogger? logger = null, bool isEnabledLoggingJsonParseErrors = false)
        : this(logger, isEnabledLoggingJsonParseErrors, 2097164) { }
    private readonly ILogger? _logger = logger;
    private readonly bool _isEnabledLoggingJsonParseErrors = isEnabledLoggingJsonParseErrors;
    private const int MaxDepth = 64;

    public override object? Read(
        ref Utf8JsonReader reader,
        Type typeToConvert,
        JsonSerializerOptions options
    ) => JsonSerializer.Deserialize<JsonElement>(ref reader, options);

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

        JsonSerializer.Serialize(writer, value, runtimeType, options);
    }

    private void WriteJTokenDirectly(Utf8JsonWriter writer, JToken token, int depth)
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

            case JTokenType.Raw:
                WriteRawToken(writer, token, depth);
                break;

            case JTokenType.Uri:
                writer.WriteStringValue(token.Value<Uri>()?.OriginalString);
                break;

            case JTokenType.Undefined:
                writer.WriteNullValue();
                break;

            default:
                throw new NotSupportedException($"Unsupported monitoring token type: {token.Type}.");
        }
    }

    // Validate before writing so malformed raw text cannot leave a partially emitted JSON value.
    // Two forward-only passes preserve Newtonsoft's normalization without copying a token tree.
    private void WriteRawToken(Utf8JsonWriter writer, JToken token, int depth)
    {
        var rawValue = ((JValue)token).Value;
        var rawText = rawValue switch
        {
            null => string.Empty,
            string text => text,
            sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal or bool or char
                => Convert.ToString(rawValue, System.Globalization.CultureInfo.CurrentCulture)!,
            _ => throw new NotSupportedException("Unsupported raw JSON value.")
        };
        if (rawText.Length > maximumRawCharacters) throw new MessageTooLargeException();

        try
        {
            using var validation = CreateRawReader(rawText, depth);
            var hasValue = false;
            var balance = 0;
            while (validation.Read())
            {
                if (validation.TokenType != Newtonsoft.Json.JsonToken.Comment) hasValue = true;
                if (validation.TokenType is Newtonsoft.Json.JsonToken.StartObject or Newtonsoft.Json.JsonToken.StartArray)
                    balance++;
                if (validation.TokenType is Newtonsoft.Json.JsonToken.EndObject or Newtonsoft.Json.JsonToken.EndArray)
                    balance--;
            }
            if (!hasValue || balance != 0)
                throw new Newtonsoft.Json.JsonReaderException("Incomplete raw JSON.");
        }
        catch (Newtonsoft.Json.JsonException)
        {
            if (_isEnabledLoggingJsonParseErrors)
            {
                _logger?.Warning("Failed to parse monitoring raw JSON token ({CharacterCount} characters).", rawText.Length);
            }
            writer.WriteStringValue(rawText);
            return;
        }

        using var reader = CreateRawReader(rawText, depth);
        while (reader.Read())
        {
            switch (reader.TokenType)
            {
                case Newtonsoft.Json.JsonToken.StartObject: writer.WriteStartObject(); break;
                case Newtonsoft.Json.JsonToken.EndObject: writer.WriteEndObject(); break;
                case Newtonsoft.Json.JsonToken.StartArray: writer.WriteStartArray(); break;
                case Newtonsoft.Json.JsonToken.EndArray: writer.WriteEndArray(); break;
                case Newtonsoft.Json.JsonToken.PropertyName: writer.WritePropertyName((string)reader.Value!); break;
                case Newtonsoft.Json.JsonToken.String: writer.WriteStringValue((string?)reader.Value); break;
                case Newtonsoft.Json.JsonToken.Integer:
                    if (reader.Value is long number) writer.WriteNumberValue(number);
                    else throw new NotSupportedException("Unsupported raw JSON integer.");
                    break;
                case Newtonsoft.Json.JsonToken.Float:
                    var value = Convert.ToDouble(reader.Value, System.Globalization.CultureInfo.InvariantCulture);
                    if (double.IsFinite(value)) writer.WriteNumberValue(value);
                    else writer.WriteNullValue();
                    break;
                case Newtonsoft.Json.JsonToken.Boolean: writer.WriteBooleanValue((bool)reader.Value!); break;
                case Newtonsoft.Json.JsonToken.Date:
                    if (reader.Value is DateTimeOffset offset) writer.WriteStringValue(offset);
                    else writer.WriteStringValue((DateTime)reader.Value!);
                    break;
                case Newtonsoft.Json.JsonToken.Null:
                case Newtonsoft.Json.JsonToken.Undefined: writer.WriteNullValue(); break;
                case Newtonsoft.Json.JsonToken.Comment: break;
                default: throw new NotSupportedException("Unsupported raw JSON token.");
            }
        }
    }

    private static Newtonsoft.Json.JsonTextReader CreateRawReader(string text, int depth) =>
        new(new StringReader(text)) { MaxDepth = Math.Max(1, MaxDepth - depth), CloseInput = true };
}
