using System.Text;
using Take.Elephant;

namespace Blip.Ai.Bot.Monitoring.Logging.Serialization;

internal sealed class RawBytesSerializer : ISerializer<byte[]>
{
    public string Serialize(byte[] value) => Encoding.UTF8.GetString(value);

    public byte[] Deserialize(string value) => Encoding.UTF8.GetBytes(value);
}
