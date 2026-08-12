using System.Threading.Channels;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Newtonsoft.Json;

namespace Blip.Ai.Bot.Monitoring.Logging.Clients
{
    public class KafkaLogClient : IKafkaLogClient, IDisposable, IAsyncDisposable
    {
        private readonly KafkaOptions _options;
        private readonly IKafkaLogBatchPublisher _publisher;
        private readonly Channel<BufferedLogEntry> _channel;
        private readonly Task _worker;
        private readonly TimeSpan _batchMaxDelay;
        private readonly TimeSpan _shutdownTimeout;
        private int _disposed;

        public KafkaLogClient(KafkaOptions options)
            : this(options, new KafkaLogBatchPublisher(options)) { }

        internal KafkaLogClient(KafkaOptions options, IKafkaLogBatchPublisher publisher)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));

            if (!_options.IsValid())
            {
                throw new ArgumentException("Kafka FireHose options are invalid.", nameof(options));
            }

            _batchMaxDelay = TimeSpan.FromMilliseconds(_options.BatchMaxDelayMilliseconds);
            _shutdownTimeout = TimeSpan.FromMilliseconds(_options.ShutdownTimeoutMilliseconds);
            _channel = Channel.CreateBounded<BufferedLogEntry>(
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
            object logEntry,
            CancellationToken cancellationToken = default
        )
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);

            if (_worker.IsFaulted)
            {
                await _worker.ConfigureAwait(false);
            }

            var json = JsonConvert.SerializeObject(logEntry);
            var bufferedLogEntry = new BufferedLogEntry(
                logEntry,
                System.Text.Encoding.UTF8.GetByteCount(json)
            );

            await _channel
                .Writer.WriteAsync(bufferedLogEntry, cancellationToken)
                .ConfigureAwait(false);
        }

        public void Dispose()
        {
            DisposeAsync().AsTask().GetAwaiter().GetResult();
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
            finally
            {
                _publisher.Dispose();
            }

            GC.SuppressFinalize(this);
        }

        private async Task ProcessQueueAsync()
        {
            var batch = new List<object>();
            var batchBytes = 0;
            var batchStartedAt = DateTime.UtcNow;

            while (true)
            {
                var readResult = await TryReadNextAsync(batch.Count > 0, batchStartedAt)
                    .ConfigureAwait(false);
                if (readResult.IsTimedOut)
                {
                    await FlushAsync(batch, CancellationToken.None).ConfigureAwait(false);
                    batchBytes = 0;
                    continue;
                }

                if (readResult.IsCompleted)
                {
                    break;
                }

                var entry = readResult.Entry!;
                if (batch.Count > 0 && batchBytes + entry.SizeInBytes > _options.BatchMaxBytes)
                {
                    await FlushAsync(batch, CancellationToken.None).ConfigureAwait(false);
                    batchBytes = 0;
                }

                if (batch.Count == 0)
                {
                    batchStartedAt = DateTime.UtcNow;
                }

                batch.Add(entry.Value);
                batchBytes += entry.SizeInBytes;

                if (batchBytes >= _options.BatchMaxBytes)
                {
                    await FlushAsync(batch, CancellationToken.None).ConfigureAwait(false);
                    batchBytes = 0;
                }
            }

            if (batch.Count > 0)
            {
                await FlushAsync(batch, CancellationToken.None).ConfigureAwait(false);
            }
        }

        private async Task<ReadResult> TryReadNextAsync(
            bool hasPendingBatch,
            DateTime batchStartedAt
        )
        {
            if (!hasPendingBatch)
            {
                return await ReadOrCompletedAsync().ConfigureAwait(false);
            }

            var elapsed = DateTime.UtcNow - batchStartedAt;
            var remainingDelay = _batchMaxDelay - elapsed;
            if (remainingDelay <= TimeSpan.Zero)
            {
                return ReadResult.TimedOut;
            }

            var readTask = ReadOrCompletedAsync().AsTask();
            var delayTask = Task.Delay(remainingDelay);
            var completedTask = await Task.WhenAny(readTask, delayTask).ConfigureAwait(false);
            return completedTask == delayTask
                ? ReadResult.TimedOut
                : await readTask.ConfigureAwait(false);
        }

        private async ValueTask<ReadResult> ReadOrCompletedAsync()
        {
            while (await _channel.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                if (_channel.Reader.TryRead(out var entry))
                {
                    return ReadResult.FromEntry(entry);
                }
            }

            return ReadResult.Completed;
        }

        private async Task FlushAsync(List<object> batch, CancellationToken cancellationToken)
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
                catch when (attempt < _options.PublishRetryCount)
                {
                    await Task.Delay(GetRetryDelay(attempt), cancellationToken)
                        .ConfigureAwait(false);
                }
            }
        }

        private static TimeSpan GetRetryDelay(int attempt) =>
            TimeSpan.FromMilliseconds(Math.Min(1000, 50 * (attempt + 1)));

        private sealed record BufferedLogEntry(object Value, int SizeInBytes);

        private sealed record ReadResult(BufferedLogEntry? Entry, bool IsTimedOut, bool IsCompleted)
        {
            public static ReadResult TimedOut { get; } = new(null, true, false);

            public static ReadResult Completed { get; } = new(null, false, true);

            public static ReadResult FromEntry(BufferedLogEntry entry) => new(entry, false, false);
        }
    }
}
