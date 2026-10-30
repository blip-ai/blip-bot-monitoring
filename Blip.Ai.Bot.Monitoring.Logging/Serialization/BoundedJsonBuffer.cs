using System.Buffers;

namespace Blip.Ai.Bot.Monitoring.Logging.Serialization;

internal sealed class MessageTooLargeException : Exception;

/// <summary>Never honors a serializer growth request beyond the bounded workspace.</summary>
internal sealed class BoundedJsonBuffer(int maximumBytes) : IBufferWriter<byte>
{
    private byte[] _buffer = new byte[Math.Min(4096, maximumBytes)];
    private readonly int _maximumWorkspace = checked(maximumBytes * 6 + 4096);
    private int _written;
    public byte[] ToArray() => _buffer.AsSpan(0, _written).ToArray();
    public void Advance(int count)
    {
        if (count < 0 || count > maximumBytes - _written)
            throw new MessageTooLargeException();
        _written += count;
    }
    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        if (sizeHint < 0 || Math.Max(1, sizeHint) > _maximumWorkspace - _written)
            throw new MessageTooLargeException();
        if (Math.Max(1, sizeHint) > _buffer.Length - _written)
        {
            var required = _written + Math.Max(1, sizeHint);
            var capacity = (int)Math.Min(_maximumWorkspace, Math.Max((long)required, (long)_buffer.Length * 2));
            Array.Resize(ref _buffer, capacity);
        }
        return _buffer.AsMemory(_written);
    }
    public Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;
}
