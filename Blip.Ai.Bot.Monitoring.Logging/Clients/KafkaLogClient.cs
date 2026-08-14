using System.Text.Json;
using System.Threading.Channels;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Serilog;

namespace Blip.Ai.Bot.Monitoring.Logging.Clients
{
    public class KafkaLogClient : IKafkaLogClient, IDisposable, IAsyncDisposable
    {
        private readonly KafkaOptions _options;
        private readonly IKafkaLogBatchPublisher _publisher;
        private readonly Channel<KafkaLogPayload> _channel;
        private readonly Task _worker;
        private readonly TimeSpan _batchMaxDelay;
        private readonly TimeSpan _shutdownTimeout;
        private readonly CancellationTokenSource _workerCts = new();
        private readonly ILogger? _logger;
        private int _disposed;

        public KafkaLogClient(KafkaOptions options, ILogger? logger = null)
            : this(options, new KafkaLogBatchPublisher(options), logger) { }

        internal KafkaLogClient(KafkaOptions options, IKafkaLogBatchPublisher publisher, ILogger? logger = null)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
            _logger = logger;

            if (!_options.IsValid())
            {
                throw new ArgumentException("Kafka options are invalid.", nameof(options));
            }

            _batchMaxDelay = TimeSpan.FromMilliseconds(_options.BatchMaxDelayMilliseconds);
            _shutdownTimeout = TimeSpan.FromMilliseconds(_options.ShutdownTimeoutMilliseconds);
            _channel = Channel.CreateBounded<KafkaLogPayload>(
                new BoundedChannelOptions(_options.QueueCapacity)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    FullMode = BoundedChannelFullMode.Wait,
                }
            );

            _worker = Task.Run(ProcessQueueAsync);
        }

        public async Task SendLogAsync(
            KafkaLogPayload logEntry,
            CancellationToken cancellationToken = default
        )
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);

            if (_worker.IsFaulted)
            {
                _logger?.Error(
                    _worker.Exception,
                    "[{Source}] The Kafka worker task faulted. No more logs will be sent.",
                    nameof(KafkaLogClient)
                );
                await _worker.ConfigureAwait(false);
            }

            await _channel.Writer.WriteAsync(logEntry, cancellationToken).ConfigureAwait(false);
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _channel.Writer.TryComplete();
            try
            {
                if (!_worker.Wait(_shutdownTimeout))
                {
                    _workerCts.Cancel();
                    try
                    {
                        _worker.Wait();
                    }
                    catch { }
                }
            }
            finally
            {
                _workerCts.Dispose();
                _publisher.Dispose();
            }

            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _channel.Writer.TryComplete();

            using var cts = new CancellationTokenSource(_shutdownTimeout);
            try
            {
                await _worker.WaitAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                await _workerCts.CancelAsync().ConfigureAwait(false);
                try
                {
                    await _worker.ConfigureAwait(false);
                }
                catch { }
            }
            finally
            {
                _workerCts.Dispose();
                _publisher.Dispose();
            }

            GC.SuppressFinalize(this);
        }

        private async Task ProcessQueueAsync()
        {
            var batch = new List<byte[]>();
            var batchBytes = 0;
            var batchStartedAt = DateTime.UtcNow;

            while (true)
            {
                var readResult = await TryReadNextAsync(batch.Count > 0, batchStartedAt)
                    .ConfigureAwait(false);

                if (readResult.IsTimedOut)
                {
                    await FlushAsync(batch, _workerCts.Token).ConfigureAwait(false);
                    batchBytes = 0;
                    continue;
                }

                if (readResult.IsCompleted)
                {
                    break;
                }

                var serialized = JsonSerializer.SerializeToUtf8Bytes(readResult.Entry!);
                var sizeInBytes = serialized.Length;

                if (batch.Count > 0 && batchBytes + sizeInBytes > _options.BatchMaxBytes)
                {
                    await FlushAsync(batch, _workerCts.Token).ConfigureAwait(false);
                    batchBytes = 0;
                }

                if (batch.Count == 0)
                {
                    batchStartedAt = DateTime.UtcNow;
                }

                batch.Add(serialized);
                batchBytes += sizeInBytes;

                if (batchBytes >= _options.BatchMaxBytes)
                {
                    await FlushAsync(batch, _workerCts.Token).ConfigureAwait(false);
                    batchBytes = 0;
                }
            }

            if (batch.Count > 0)
            {
                await FlushAsync(batch, _workerCts.Token).ConfigureAwait(false);
            }
        }

        private async Task<ReadResult> TryReadNextAsync(
            bool hasPendingBatch,
            DateTime batchStartedAt
        )
        {
            if (!hasPendingBatch)
            {
                return await ReadOrCompletedAsync(_workerCts.Token).ConfigureAwait(false);
            }

            var elapsed = DateTime.UtcNow - batchStartedAt;
            var remainingDelay = _batchMaxDelay - elapsed;
            if (remainingDelay <= TimeSpan.Zero)
            {
                return ReadResult.TimedOut;
            }

            using var batchCts = CancellationTokenSource.CreateLinkedTokenSource(_workerCts.Token);
            batchCts.CancelAfter(remainingDelay);
            try
            {
                return await ReadOrCompletedAsync(batchCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!_workerCts.IsCancellationRequested)
            {
                return ReadResult.TimedOut;
            }
        }

        private async ValueTask<ReadResult> ReadOrCompletedAsync(
            CancellationToken cancellationToken
        )
        {
            while (await _channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (_channel.Reader.TryRead(out var entry))
                {
                    return ReadResult.FromEntry(entry);
                }
            }

            return ReadResult.Completed;
        }

        private async Task FlushAsync(List<byte[]> batch, CancellationToken cancellationToken)
        {
            if (batch.Count == 0)
            {
                return;
            }

            var kafkaLogBatch = new KafkaLogBatch
            {
                Events = batch.ToArray(),
                Datetime = DateTime.UtcNow,
            };

            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    await _publisher
                        .PublishAsync(kafkaLogBatch, cancellationToken)
                        .ConfigureAwait(false);
                    batch.Clear();
                    return;
                }
                catch (OperationCanceledException)
                {
                    _logger?.Warning(
                        "[{Source}] Kafka batch publish canceled. BatchSize={BatchSize}",
                        nameof(KafkaLogClient),
                        kafkaLogBatch.Events.Length
                    );
                    throw;
                }
                catch (Exception retryEx) when (attempt < _options.PublishRetryCount)
                {
                    var delay = GetRetryDelay(attempt);
                    _logger?.Warning(
                        retryEx,
                        "[{Source}] Kafka batch publish failed on attempt {Attempt}/{MaxRetries}. Retrying in {DelayMs}ms. BatchSize={BatchSize}",
                        nameof(KafkaLogClient),
                        attempt + 1,
                        _options.PublishRetryCount,
                        (int)delay.TotalMilliseconds,
                        kafkaLogBatch.Events.Length
                    );
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception discardEx)
                {
                    _logger?.Error(
                        discardEx,
                        "[{Source}] Kafka batch DISCARDED after {MaxRetries} retries. BatchSize={BatchSize}",
                        nameof(KafkaLogClient),
                        _options.PublishRetryCount,
                        kafkaLogBatch.Events.Length
                    );
                    batch.Clear();
                    return;
                }
            }
        }

        private static TimeSpan GetRetryDelay(int attempt) =>
            TimeSpan.FromMilliseconds(Math.Min(1000, 50 * (attempt + 1)));

        private sealed record ReadResult(KafkaLogPayload? Entry, bool IsTimedOut, bool IsCompleted)
        {
            public static ReadResult TimedOut { get; } = new(null, true, false);

            public static ReadResult Completed { get; } = new(null, false, true);

            public static ReadResult FromEntry(KafkaLogPayload entry) => new(entry, false, false);
        }
    }
}
