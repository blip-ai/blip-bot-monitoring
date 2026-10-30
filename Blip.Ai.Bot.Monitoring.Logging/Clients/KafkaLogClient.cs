using System.Diagnostics;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Threading.Channels;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Blip.Ai.Bot.Monitoring.Logging.Serialization;
using Serilog;

namespace Blip.Ai.Bot.Monitoring.Logging.Clients;

public class KafkaLogClient : ILazyKafkaLogClient, IDisposable, IAsyncDisposable
{
    private readonly KafkaOptions _options;
    private readonly IKafkaLogBatchPublisher _publisher;
    private readonly Channel<SerializedEvent> _channel;
    private readonly Task _worker;
    private readonly CancellationTokenSource _stop = new();
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly object _gate = new();
    private readonly SemaphoreSlim _serializers;
    private long _reservedBytes;
    private int _reservedCount;
    private bool _disposed;
    private Task? _shutdown;

    public MonitoringDiagnostics Diagnostics { get; } = new();
    public long RetainedBytes { get { lock (_gate) return _reservedBytes; } }
    public int RetainedCount { get { lock (_gate) return _reservedCount; } }

    public KafkaLogClient(KafkaOptions options, ILogger? logger = null,
        bool enabledLoggingJsonErrors = false)
        : this(options, CreatePublisher(options), logger, enabledLoggingJsonErrors) { }

    private static IKafkaLogBatchPublisher CreatePublisher(KafkaOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!options.IsValid()) throw new ArgumentException("Kafka options are invalid.", nameof(options));
        return new KafkaLogBatchPublisher(options);
    }

    internal KafkaLogClient(KafkaOptions options, IKafkaLogBatchPublisher publisher,
        ILogger? logger = null, bool enabledLoggingJsonErrors = false)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(publisher);
        if (!options.IsValid()) throw new ArgumentException("Kafka options are invalid.", nameof(options));
        // A caller mutating configuration later must not invalidate reservations.
        _options = new KafkaOptions
        {
            QueueCapacity = options.QueueCapacity, QueueMaxBytes = options.QueueMaxBytes,
            MaxMessageBytes = options.MaxMessageBytes, BatchMaxBytes = options.BatchMaxBytes,
            BatchMaxCount = options.BatchMaxCount, BatchMaxDelayMilliseconds = options.BatchMaxDelayMilliseconds,
            ShutdownTimeoutMilliseconds = options.ShutdownTimeoutMilliseconds,
            DeliveryTimeoutMilliseconds = options.DeliveryTimeoutMilliseconds,
            MaxEventAgeMilliseconds = options.MaxEventAgeMilliseconds
        };
        _publisher = publisher;
        _serializers = new SemaphoreSlim(options.MaxConcurrentSerializations);
        _jsonOptions = new JsonSerializerOptions
        {
            Converters = { new ObjectJsonConverter(logger, enabledLoggingJsonErrors, options.MaxMessageBytes) },
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };
        _channel = Channel.CreateBounded<SerializedEvent>(new BoundedChannelOptions(options.QueueCapacity)
        {
            SingleReader = true, SingleWriter = false, FullMode = BoundedChannelFullMode.Wait
        });
        _worker = Task.Run(ProcessQueueAsync);
    }

    /// <summary>Best-effort, nonwaiting admission. Completion does not imply delivery.</summary>
    public Task SendLogAsync(KafkaLogPayload logEntry, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(logEntry);
        TrySendLog(() => logEntry, cancellationToken);
        return Task.CompletedTask;
    }

    public bool TrySendLog(Func<KafkaLogPayload?> payloadFactory, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(payloadFactory);
        cancellationToken.ThrowIfCancellationRequested();
        var admittedAt = Stopwatch.GetTimestamp();
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_reservedCount >= _options.QueueCapacity ||
                _reservedBytes > _options.QueueMaxBytes - _options.MaxMessageBytes)
            {
                Diagnostics.Record(MonitoringOutcome.Full);
                return false;
            }
            if (!_serializers.Wait(0))
            {
                Diagnostics.Record(MonitoringOutcome.Full);
                return false;
            }
            _reservedCount++;
            _reservedBytes += _options.MaxMessageBytes;
            MonitoringDiagnostics.Reserve(_options.MaxMessageBytes, 1);
        }

        var reservation = _options.MaxMessageBytes;
        try
        {
            var logEntry = payloadFactory();
            if (logEntry == null) return false;
            var buffer = new BoundedJsonBuffer(_options.MaxMessageBytes);
            using (var writer = new Utf8JsonWriter(buffer,
                new JsonWriterOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping }))
            {
                JsonSerializer.Serialize(writer, logEntry, _jsonOptions);
            }
            var bytes = buffer.ToArray();
            lock (_gate)
            {
                _reservedBytes -= reservation - bytes.Length;
                MonitoringDiagnostics.Reserve(bytes.Length - reservation, 0);
                reservation = bytes.Length;
                if (!_disposed && !cancellationToken.IsCancellationRequested &&
                    _channel.Writer.TryWrite(new SerializedEvent(bytes, admittedAt)))
                {
                    reservation = -1; // Worker now owns the count and exact-byte reservation.
                    Diagnostics.Record(MonitoringOutcome.Admitted);
                    return true;
                }
                else Diagnostics.Record(cancellationToken.IsCancellationRequested && !_disposed
                    ? MonitoringOutcome.Canceled : MonitoringOutcome.Shutdown);
            }
        }
        catch (MessageTooLargeException) { Diagnostics.Record(MonitoringOutcome.Oversize); }
        catch (Exception) { Diagnostics.Record(MonitoringOutcome.SerializationError); }
        finally
        {
            if (reservation >= 0) Release(reservation);
            _serializers.Release();
        }
        return false;
    }

    private void Release(int bytes)
    {
        lock (_gate)
        {
            _reservedCount--; _reservedBytes -= bytes;
            MonitoringDiagnostics.Reserve(-bytes, -1);
        }
    }

    private async Task ProcessQueueAsync()
    {
        var batch = new List<SerializedEvent>();
        var batchBytes = 0L;
        var started = Stopwatch.GetTimestamp();
        try
        {
            while (true)
            {
                _stop.Token.ThrowIfCancellationRequested();
                if (batch.Count > 0 &&
                    Stopwatch.GetElapsedTime(started).TotalMilliseconds >= _options.BatchMaxDelayMilliseconds)
                {
                    await FlushAsync(batch).ConfigureAwait(false);
                    batchBytes = 0;
                }
                if (_channel.Reader.TryRead(out var entry))
                {
                    if (IsExpired(entry))
                    {
                        Release(entry.Bytes.Length);
                        Diagnostics.Record(MonitoringOutcome.Expired);
                        continue;
                    }
                    if (batch.Count > 0 && batchBytes + entry.Bytes.Length > _options.BatchMaxBytes)
                    {
                        // Keep this dequeued event charged while the previous batch is in flight.
                        try { await FlushAsync(batch).ConfigureAwait(false); }
                        catch { Release(entry.Bytes.Length); Diagnostics.Record(MonitoringOutcome.Shutdown); throw; }
                        batchBytes = 0;
                    }
                    if (batch.Count == 0) started = Stopwatch.GetTimestamp();
                    batch.Add(entry);
                    batchBytes += entry.Bytes.Length;
                    if (batch.Count >= _options.BatchMaxCount || batchBytes >= _options.BatchMaxBytes)
                    {
                        await FlushAsync(batch).ConfigureAwait(false);
                        batchBytes = 0;
                    }
                    continue;
                }
                if (batch.Count == 0)
                {
                    if (!await _channel.Reader.WaitToReadAsync(_stop.Token).ConfigureAwait(false)) break;
                }
                else
                {
                    var remaining = Math.Max(1, _options.BatchMaxDelayMilliseconds -
                        Stopwatch.GetElapsedTime(started).TotalMilliseconds);
                    using var delay = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
                    delay.CancelAfter(TimeSpan.FromMilliseconds(remaining));
                    try
                    {
                        if (!await _channel.Reader.WaitToReadAsync(delay.Token).ConfigureAwait(false)) break;
                    }
                    catch (OperationCanceledException) when (!_stop.IsCancellationRequested) { }
                }
            }
            await FlushAsync(batch).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        finally
        {
            _channel.Writer.TryComplete();
            foreach (var bytes in batch) { Release(bytes.Bytes.Length); Diagnostics.Record(MonitoringOutcome.Shutdown); }
            while (_channel.Reader.TryRead(out var bytes))
            {
                Release(bytes.Bytes.Length);
                Diagnostics.Record(MonitoringOutcome.Shutdown);
            }
        }
    }

    private bool IsExpired(SerializedEvent entry) =>
        Stopwatch.GetElapsedTime(entry.CreatedAt).TotalMilliseconds >= _options.MaxEventAgeMilliseconds;

    private async Task FlushAsync(List<SerializedEvent> batch)
    {
        if (batch.Count == 0) return;
        var events = batch.Where(entry => !IsExpired(entry)).Select(entry => entry.Bytes).ToArray();
        var expired = batch.Count - events.Length;
        if (expired > 0) Diagnostics.Record(MonitoringOutcome.Expired, expired);
        try
        {
            // Never replay an entire batch: an earlier record may already have reached Kafka.
            if (events.Length > 0)
            {
                await _publisher.PublishAsync(new KafkaLogBatch { Events = events, Datetime = DateTime.UtcNow },
                    _stop.Token).ConfigureAwait(false);
                Diagnostics.Record(MonitoringOutcome.Delivered, events.Length);
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
            Diagnostics.Record(MonitoringOutcome.Shutdown, events.Length);
            throw;
        }
        catch (PartialBatchException ex)
        {
            Diagnostics.Record(MonitoringOutcome.Delivered, ex.Delivered);
            Diagnostics.Record(MonitoringOutcome.DeliveryFailed, events.Length - ex.Delivered);
        }
        catch (Exception) { Diagnostics.Record(MonitoringOutcome.DeliveryFailed, events.Length); }
        finally
        {
            foreach (var entry in batch) Release(entry.Bytes.Length);
            batch.Clear();
        }
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public ValueTask DisposeAsync() =>
        DisposeAsync(TimeSpan.FromMilliseconds(_options.ShutdownTimeoutMilliseconds));

    internal ValueTask DisposeAsync(TimeSpan timeout)
    {
        lock (_gate)
        {
            if (_shutdown != null) return new ValueTask(_shutdown);
            _disposed = true;
            _channel.Writer.TryComplete();
            _shutdown = ShutdownAsync(timeout < TimeSpan.FromMilliseconds(_options.ShutdownTimeoutMilliseconds)
                ? timeout : TimeSpan.FromMilliseconds(_options.ShutdownTimeoutMilliseconds));
            return new ValueTask(_shutdown);
        }
    }

    private async Task ShutdownAsync(TimeSpan timeout)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            await _worker.WaitAsync(timeout)
                .ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            var cancellation = _stop.CancelAsync();
            _ = cancellation.ContinueWith(t => { _ = t.Exception; }, CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            // Do not dispose a publisher concurrently with delivery. A non-cooperative injected
            // publisher remains charged and is cleaned up only when its operation actually ends.
            Diagnostics.Record(MonitoringOutcome.Abandoned, RetainedCount);
            _ = Task.WhenAll(_worker, cancellation).ContinueWith(t => { _ = t.Exception; DisposePublisher(); },
                CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default);
            return;
        }
        catch (Exception) { Diagnostics.Record(MonitoringOutcome.Shutdown); }
        finally { GC.SuppressFinalize(this); }
        var cleanup = Task.Run(DisposePublisher);
        var remaining = timeout.TotalMilliseconds - Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        if (remaining > 0)
        {
            try { await cleanup.WaitAsync(TimeSpan.FromMilliseconds(remaining)).ConfigureAwait(false); }
            catch (TimeoutException) { }
        }
    }

    private void DisposePublisher()
    {
        try { _publisher.Dispose(); }
        catch (Exception) { Diagnostics.Record(MonitoringOutcome.Shutdown); }
        finally { _stop.Dispose(); }
    }

    private sealed record SerializedEvent(byte[] Bytes, long CreatedAt);
}
