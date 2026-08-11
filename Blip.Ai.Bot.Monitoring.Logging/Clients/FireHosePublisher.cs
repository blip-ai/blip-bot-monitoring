using System.Threading.Channels;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;

namespace Blip.Ai.Bot.Monitoring.Logging.Clients
{
    /// <summary>
    /// A buffered publisher that enqueues log entries into an in-memory channel and
    /// drains them in batches via a background worker, reducing the number of HTTP
    /// round-trips to the FireHose endpoint.
    /// </summary>
    public sealed class FireHosePublisher : IFireHosePublisher
    {
        private readonly IFireHoseClient _client;
        private readonly int _batchSize;
        private readonly int _flushIntervalMs;
        private readonly Channel<object> _channel;
        private readonly Task _backgroundTask;

        /// <summary>
        /// Initializes a new instance of <see cref="FireHosePublisher"/>.
        /// </summary>
        /// <param name="client">The FireHose HTTP client used to send batches.</param>
        /// <param name="options">Optional FireHose options for buffer configuration. Defaults are used when null.</param>
        public FireHosePublisher(IFireHoseClient client, FireHoseOptions? options = null)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));

            var capacity = options?.ChannelCapacity ?? 10_000;
            _batchSize = options?.BatchSize > 0 ? options.BatchSize : 100;
            _flushIntervalMs = options?.FlushIntervalMs > 0 ? options.FlushIntervalMs : 500;

            _channel = Channel.CreateBounded<object>(
                new BoundedChannelOptions(capacity)
                {
                    FullMode = BoundedChannelFullMode.DropOldest,
                    SingleReader = true,
                }
            );

            _backgroundTask = Task.Run(DrainAsync);
        }

        /// <inheritdoc />
        public void Publish(object logEntry)
        {
            if (logEntry is null)
                return;

            _channel.Writer.TryWrite(logEntry);
        }

        private async Task DrainAsync()
        {
            var reader = _channel.Reader;
            var batch = new List<object>(_batchSize);

            while (true)
            {
                batch.Clear();

                bool channelOpen;
                try
                {
                    channelOpen = await reader.WaitToReadAsync().ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                if (!channelOpen)
                    break;

                while (batch.Count < _batchSize && reader.TryRead(out var item))
                    batch.Add(item);

                if (batch.Count < _batchSize)
                {
                    using var cts = new CancellationTokenSource(_flushIntervalMs);
                    try
                    {
                        while (batch.Count < _batchSize)
                        {
                            var hasMore = await reader
                                .WaitToReadAsync(cts.Token)
                                .ConfigureAwait(false);
                            if (!hasMore)
                                break;
                            while (batch.Count < _batchSize && reader.TryRead(out var more))
                                batch.Add(more);
                        }
                    }
                    catch (OperationCanceledException) { }
                }

                if (batch.Count > 0)
                    await SendBatchSafeAsync(batch).ConfigureAwait(false);
            }

            batch.Clear();
            while (reader.TryRead(out var remaining))
                batch.Add(remaining);

            if (batch.Count > 0)
                await SendBatchSafeAsync(batch).ConfigureAwait(false);
        }

        private async Task SendBatchSafeAsync(List<object> batch)
        {
            var snapshot = new List<object>(batch);
            try
            {
                await _client
                    .SendBatchToFireHoseAsync(snapshot.AsReadOnly())
                    .ConfigureAwait(false);
            }
            catch
            {
                // Swallow to prevent crashing the background worker.
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _channel.Writer.TryComplete();
            try
            {
                _backgroundTask.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException) { }

            GC.SuppressFinalize(this);
        }
    }
}
