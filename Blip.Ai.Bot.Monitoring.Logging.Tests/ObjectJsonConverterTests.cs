using System.Text.Json;
using Blip.Ai.Bot.Monitoring.Logging.Serialization;
using Newtonsoft.Json.Linq;

namespace Blip.Ai.Bot.Monitoring.Logging.Tests;

public class ObjectJsonConverterTests
{
    private readonly JsonSerializerOptions _options = new()
    {
        Converters = { new ObjectJsonConverter() }
    };

    private string SerializeAsObject(object? value) =>
        JsonSerializer.Serialize<object>(value!, _options);

    // ── JToken: Object ─────────────────────────────────────────────────────

    [Fact]
    public void Write_WithJTokenObject_ShouldSerializeProperties()
    {
        var jObject = new JObject { ["name"] = "test", ["count"] = 42 };

        var json = SerializeAsObject(jObject);

        using var doc = JsonDocument.Parse(json);
        Assert.Equal("test", doc.RootElement.GetProperty("name").GetString());
        Assert.Equal(42, doc.RootElement.GetProperty("count").GetInt32());
    }

    [Fact]
    public void Write_WithJTokenObjectNullProperty_ShouldWriteNullForNullValues()
    {
        var jObject = new JObject { ["key"] = JValue.CreateNull() };

        var json = SerializeAsObject(jObject);

        using var doc = JsonDocument.Parse(json);
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("key").ValueKind);
    }

    // ── JToken: Array ──────────────────────────────────────────────────────

    [Fact]
    public void Write_WithJTokenArray_ShouldSerializeElements()
    {
        var jArray = new JArray(1, "two", true);

        var json = SerializeAsObject(jArray);

        using var doc = JsonDocument.Parse(json);
        Assert.Equal(JsonValueKind.Array, doc.RootElement.ValueKind);
        Assert.Equal(3, doc.RootElement.GetArrayLength());
        Assert.Equal(1, doc.RootElement[0].GetInt32());
        Assert.Equal("two", doc.RootElement[1].GetString());
        Assert.True(doc.RootElement[2].GetBoolean());
    }

    // ── JToken: Integer ────────────────────────────────────────────────────

    [Fact]
    public void Write_WithJTokenIntegerLong_ShouldSerializeAsNumber()
    {
        var jToken = new JValue(long.MaxValue);

        var json = SerializeAsObject(jToken);

        Assert.Equal(long.MaxValue.ToString(), json);
    }

    [Fact]
    public void Write_WithJTokenIntegerUlongMax_ShouldSerializeWithoutOverflow()
    {
        // ulong.MaxValue exceeds long.MaxValue — previous code silently produced a negative number
        var jToken = new JValue(ulong.MaxValue);

        var json = SerializeAsObject(jToken);

        Assert.Equal(ulong.MaxValue.ToString(), json);
    }

    // ── JToken: Float ──────────────────────────────────────────────────────

    [Fact]
    public void Write_WithJTokenFloat_ShouldSerializeDouble()
    {
        var jToken = new JValue(3.14);

        var json = SerializeAsObject(jToken);

        var value = double.Parse(json, System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal(3.14, value, precision: 10);
    }

    [Fact]
    public void Write_WithJTokenFloatNaN_ShouldWriteNull()
    {
        // double.NaN is not valid JSON — previous code threw ArgumentException via Utf8JsonWriter
        var jToken = new JValue(double.NaN);

        var json = SerializeAsObject(jToken);

        Assert.Equal("null", json);
    }

    [Fact]
    public void Write_WithJTokenFloatPositiveInfinity_ShouldWriteNull()
    {
        var jToken = new JValue(double.PositiveInfinity);

        var json = SerializeAsObject(jToken);

        Assert.Equal("null", json);
    }

    [Fact]
    public void Write_WithJTokenFloatNegativeInfinity_ShouldWriteNull()
    {
        var jToken = new JValue(double.NegativeInfinity);

        var json = SerializeAsObject(jToken);

        Assert.Equal("null", json);
    }

    // ── JToken: String ─────────────────────────────────────────────────────

    [Fact]
    public void Write_WithJTokenString_ShouldSerializeString()
    {
        var jToken = new JValue("hello world");

        var json = SerializeAsObject(jToken);

        Assert.Equal("\"hello world\"", json);
    }

    // ── JToken: Boolean ────────────────────────────────────────────────────

    [Fact]
    public void Write_WithJTokenBooleanTrue_ShouldSerializeTrue()
    {
        Assert.Equal("true", SerializeAsObject(new JValue(true)));
    }

    [Fact]
    public void Write_WithJTokenBooleanFalse_ShouldSerializeFalse()
    {
        Assert.Equal("false", SerializeAsObject(new JValue(false)));
    }

    // ── JToken: Null ───────────────────────────────────────────────────────

    [Fact]
    public void Write_WithJTokenNull_ShouldSerializeNull()
    {
        var json = SerializeAsObject(JValue.CreateNull());

        Assert.Equal("null", json);
    }

    // ── JToken: Date ───────────────────────────────────────────────────────

    [Fact]
    public void Write_WithJTokenDateAsDateTime_ShouldSerializeIso8601()
    {
        var dt = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
        var jToken = new JValue(dt);

        var json = SerializeAsObject(jToken);

        // Compare parsed value — format-agnostic, valid for both "O" and compact ISO 8601
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(dt, doc.RootElement.GetDateTime());
    }

    [Fact]
    public void Write_WithJTokenDateAsDateTimeOffset_ShouldSerializeIso8601()
    {
        var dto = new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.Zero);
        var jToken = new JValue(dto);

        var json = SerializeAsObject(jToken);

        // Compare parsed value — format-agnostic, valid for both "O" and compact ISO 8601
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(dto, doc.RootElement.GetDateTimeOffset());
    }

    // ── JToken: Bytes ──────────────────────────────────────────────────────

    [Fact]
    public void Write_WithJTokenBytes_ShouldSerializeAsBase64()
    {
        var data = new byte[] { 1, 2, 3 };
        var jToken = new JValue(data);

        var json = SerializeAsObject(jToken);

        var expected = $"\"{Convert.ToBase64String(data)}\"";
        Assert.Equal(expected, json);
    }

    // ── JToken: Guid ───────────────────────────────────────────────────────

    [Fact]
    public void Write_WithJTokenGuid_ShouldSerializeGuid()
    {
        var guid = Guid.NewGuid();
        var jToken = new JValue(guid);

        var json = SerializeAsObject(jToken);

        Assert.Equal($"\"{guid}\"", json);
    }

    // ── JToken: MaxDepth ───────────────────────────────────────────────────

    [Fact]
    public void Write_WithJTokenNestedObjectExceedingMaxDepth_ShouldWriteNullAtBoundary()
    {
        // 65 JObjects (depths 0–64): the item at depth 64 hits the MaxDepth guard
        var root = CreateNestedJObject(65);

        var json = SerializeAsObject(root);

        Assert.False(string.IsNullOrEmpty(json));
        using var doc = JsonDocument.Parse(json);
        var element = doc.RootElement;
        for (var i = 0; i < 63; i++)
            element = element.GetProperty("nested");
        Assert.Equal(JsonValueKind.Null, element.GetProperty("nested").ValueKind);
    }

    // ── Non-JToken: POCO ───────────────────────────────────────────────────

    [Fact]
    public void Write_WithPoco_ShouldSerializeProperties()
    {
        var value = new TestRecord("Alice", 30);

        var json = SerializeAsObject(value);

        using var doc = JsonDocument.Parse(json);
        Assert.Equal("Alice", doc.RootElement.GetProperty("Name").GetString());
        Assert.Equal(30, doc.RootElement.GetProperty("Age").GetInt32());
    }

    // ── Non-JToken: Boxed object (recursion guard) ─────────────────────────

    [Fact]
    public void Write_WithBoxedObject_ShouldWriteNullWithoutRecursion()
    {
        // Prior to the fix, runtimeType == typeof(object) would recurse infinitely
        var value = new object();

        var json = SerializeAsObject(value);

        Assert.Equal("null", json);
    }

    // ── Non-JToken: Fallback to string ─────────────────────────────────────

    [Fact]
    public void Write_WithNonSerializableValue_ShouldFallbackToToString()
    {
        // Delegate types are not supported by System.Text.Json — triggers the catch fallback
        var action = (Action)(() => { });

        var json = SerializeAsObject(action);

        using var doc = JsonDocument.Parse(json);
        Assert.Equal(JsonValueKind.String, doc.RootElement.ValueKind);
    }

    // ── Read ───────────────────────────────────────────────────────────────

    [Fact]
    public void Read_WithJsonObject_ShouldReturnJsonElement()
    {
        var result = JsonSerializer.Deserialize<object>("{\"key\":\"value\"}", _options);

        Assert.IsType<JsonElement>(result);
        var element = (JsonElement)result!;
        Assert.Equal("value", element.GetProperty("key").GetString());
    }

    [Fact]
    public void Read_WithJsonString_ShouldReturnJsonElement()
    {
        var result = JsonSerializer.Deserialize<object>("\"hello\"", _options);

        Assert.IsType<JsonElement>(result);
        Assert.Equal("hello", ((JsonElement)result!).GetString());
    }

    [Fact]
    public void Read_WithJsonNumber_ShouldReturnJsonElement()
    {
        var result = JsonSerializer.Deserialize<object>("42", _options);

        Assert.IsType<JsonElement>(result);
        Assert.Equal(42, ((JsonElement)result!).GetInt32());
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static JObject CreateNestedJObject(int levels)
    {
        var root = new JObject();
        var current = root;
        for (var i = 1; i < levels; i++)
        {
            var child = new JObject();
            current["nested"] = child;
            current = child;
        }
        current["value"] = "leaf";
        return root;
    }

    private sealed record TestRecord(string Name, int Age);
}
