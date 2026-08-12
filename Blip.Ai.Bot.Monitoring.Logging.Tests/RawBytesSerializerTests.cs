using System.Text;
using Blip.Ai.Bot.Monitoring.Logging.Serialization;

namespace Blip.Ai.Bot.Monitoring.Logging.Tests;

public class RawBytesSerializerTests
{
    private readonly RawBytesSerializer _serializer = new();

    [Fact]
    public void Serialize_ShouldReturnUtf8String()
    {
        var json = "{\"title\":\"test\"}";
        var bytes = Encoding.UTF8.GetBytes(json);

        var result = _serializer.Serialize(bytes);

        Assert.Equal(json, result);
    }

    [Fact]
    public void Deserialize_ShouldReturnUtf8Bytes()
    {
        var json = "{\"title\":\"test\"}";

        var result = _serializer.Deserialize(json);

        Assert.Equal(Encoding.UTF8.GetBytes(json), result);
    }

    [Fact]
    public void SerializeDeserialize_ShouldRoundTrip()
    {
        var json = "{\"FlowId\":\"abc\",\"Title\":\"roundtrip\"}";
        var original = Encoding.UTF8.GetBytes(json);

        var serialized = _serializer.Serialize(original);
        var deserialized = _serializer.Deserialize(serialized);

        Assert.Equal(original, deserialized);
    }

    [Fact]
    public void Serialize_WithEmptyBytes_ShouldReturnEmptyString()
    {
        var result = _serializer.Serialize(new byte[0]);

        Assert.Equal(string.Empty, result);
    }

    [Fact]
    public void Deserialize_WithEmptyString_ShouldReturnEmptyBytes()
    {
        var result = _serializer.Deserialize(string.Empty);

        Assert.Equal(new byte[0], result);
    }

    [Fact]
    public void Serialize_WithUnicodeCharacters_ShouldPreserveContent()
    {
        var json = "{\"title\":\"caf\u00e9 \u00e7a va\"}";
        var bytes = Encoding.UTF8.GetBytes(json);

        var serialized = _serializer.Serialize(bytes);
        var deserialized = _serializer.Deserialize(serialized);

        Assert.Equal(bytes, deserialized);
    }

    [Fact]
    public void Serialize_WithComplexPayload_ShouldPreserveAllFields()
    {
        var json =
            "{\"FlowId\":\"flow-001\",\"Title\":\"MessageProcessing\",\"Channel\":\"wa.gw.msging.net\"}";
        var bytes = Encoding.UTF8.GetBytes(json);

        var result = _serializer.Serialize(bytes);

        Assert.Equal(json, result);
    }
}
