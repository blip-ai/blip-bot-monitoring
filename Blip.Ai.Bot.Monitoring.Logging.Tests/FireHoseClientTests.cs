using System.Collections.Concurrent;
using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Models;

namespace Blip.Ai.Bot.Monitoring.Logging.Tests
{
    public class FireHoseClientTests
    {
        private static KafkaOptions CreateOptions() =>
            new()
            {
                BootstrapServers = "localhost:9092",
                Topic = "bot-monitoring",
                BatchMaxBytes = 1024 * 1024,
                BatchMaxDelayMilliseconds = 50,
                QueueCapacity = 100,
                ShutdownTimeoutMilliseconds = 5000,
            };

        [Fact]
        public void Constructor_WithNullOptions_ShouldThrowArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new FireHoseClient(null!));
        }

        [Fact]
        public void Constructor_WithInvalidKafkaOptions_ShouldThrowArgumentException()
        {
            var publisher = new CapturingFireHoseBatchPublisher();

            Assert.Throws<ArgumentException>(() => new FireHoseClient(new KafkaOptions(), publisher));
        }

        [Fact]
        public async Task SendLogToFireHoseAsync_WhenBatchSizeIsReached_ShouldPublishBatch()
        {
            var options = CreateOptions();
            options.BatchMaxBytes = 1;
            var publisher = new CapturingFireHoseBatchPublisher();
            await using var client = new FireHoseClient(options, publisher);

            await client.SendLogToFireHoseAsync(new { Message = "test" });

            var batch = await publisher.WaitForBatchAsync();
            Assert.Single(batch.Events);
        }

        [Fact]
        public async Task SendLogToFireHoseAsync_WhenBatchDelayElapses_ShouldPublishBatch()
        {
            var options = CreateOptions();
            options.BatchMaxDelayMilliseconds = 10;
            var publisher = new CapturingFireHoseBatchPublisher();
            await using var client = new FireHoseClient(options, publisher);

            await client.SendLogToFireHoseAsync(new { Message = "test" });

            var batch = await publisher.WaitForBatchAsync();
            Assert.Single(batch.Events);
        }

        [Fact]
        public async Task DisposeAsync_ShouldFlushBufferedLogsBeforeShutdown()
        {
            var options = CreateOptions();
            options.BatchMaxDelayMilliseconds = 60000;
            var publisher = new CapturingFireHoseBatchPublisher();
            var client = new FireHoseClient(options, publisher);

            await client.SendLogToFireHoseAsync(new { Message = "test" });
            await client.DisposeAsync();

            Assert.Single(publisher.Batches);
            Assert.Single(publisher.Batches.Single().Events);
        }

        private sealed class CapturingFireHoseBatchPublisher : IFireHoseBatchPublisher
        {
            private readonly TaskCompletionSource<FireHoseBatch> _published = new(
                TaskCreationOptions.RunContinuationsAsynchronously
            );

            public ConcurrentQueue<FireHoseBatch> Batches { get; } = new();

            public Task PublishAsync(FireHoseBatch batch, CancellationToken cancellationToken)
            {
                Batches.Enqueue(batch);
                _published.TrySetResult(batch);
                return Task.CompletedTask;
            }

            public async Task<FireHoseBatch> WaitForBatchAsync()
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                return await _published.Task.WaitAsync(cts.Token);
            }

            public void Dispose() { }
        }
    }
}