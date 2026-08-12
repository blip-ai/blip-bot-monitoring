using System.Collections.Concurrent;
using Blip.Ai.Bot.Monitoring.Logging.Abstractions.Models;
using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using System.Text.Json;
using Blip.Ai.Bot.Monitoring.Logging.Serialization;

namespace Blip.Ai.Bot.Monitoring.Logging.Tests
{
    public class KafkaLogClientTests
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

        private static KafkaLogPayload CreatePayload(string title = "test") =>
            KafkaLogPayload.FromInput(
                new LogInput
                {
                    FlowId = Guid.NewGuid().ToString(),
                    Title = title,
                    IdMessage = Guid.NewGuid().ToString(),
                    From = "user1",
                    OriginalFrom = "user1",
                    To = "bot",
                    OriginalTo = "bot",
                    Operation = "op",
                    EventType = "event-type",
                    StateId = Guid.NewGuid().ToString(),
                    Channel = "wa.gw.msging.net",
                    FlowVersion = 1,
                },
                Enums.LogCategory.UserInput,
                "test-cluster"
            );

        [Fact]
        public void Constructor_WithNullOptions_ShouldThrowArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new KafkaLogClient(null!));
        }

        [Fact]
        public void Constructor_WithInvalidKafkaOptions_ShouldThrowArgumentException()
        {
            var publisher = new CapturingKafkaLogBatchPublisher();

            Assert.Throws<ArgumentException>(() =>
                new KafkaLogClient(new KafkaOptions(), publisher)
            );
        }

        [Fact]
        public async Task SendLogAsync_WhenBatchSizeIsReached_ShouldPublishBatch()
        {
            var options = CreateOptions();
            options.BatchMaxBytes = 1;
            var publisher = new CapturingKafkaLogBatchPublisher();
            await using var client = new KafkaLogClient(options, publisher);

            await client.SendLogAsync(CreatePayload());

            var batch = await publisher.WaitForBatchAsync();
            Assert.Single(batch.Events);
        }

        [Fact]
        public async Task SendLogAsync_WhenBatchDelayElapses_ShouldPublishBatch()
        {
            var options = CreateOptions();
            options.BatchMaxDelayMilliseconds = 10;
            var publisher = new CapturingKafkaLogBatchPublisher();
            await using var client = new KafkaLogClient(options, publisher);

            await client.SendLogAsync(CreatePayload());

            var batch = await publisher.WaitForBatchAsync();
            Assert.Single(batch.Events);
        }

        [Fact]
        public async Task DisposeAsync_ShouldFlushBufferedLogsBeforeShutdown()
        {
            var options = CreateOptions();
            options.BatchMaxDelayMilliseconds = 60000;
            var publisher = new CapturingKafkaLogBatchPublisher();
            var client = new KafkaLogClient(options, publisher);

            await client.SendLogAsync(CreatePayload());
            await client.DisposeAsync();

            Assert.Single(publisher.Batches);
            Assert.Single(publisher.Batches.Single().Events);
        }

        [Fact]
        public async Task SendLogAsync_WhenPublishFailsWithinRetryLimit_ShouldRetryAndSucceed()
        {
            var options = CreateOptions();
            options.BatchMaxBytes = 1;
            options.PublishRetryCount = 2;
            var publisher = new FailingKafkaLogBatchPublisher(failuresBeforeSuccess: 2);
            await using var client = new KafkaLogClient(options, publisher);

            await client.SendLogAsync(CreatePayload());

            var batch = await publisher.WaitForBatchAsync();

            Assert.Equal(3, publisher.AttemptCount);
            Assert.Single(batch.Events);
        }

        [Fact]
        public async Task DisposeAsync_WhenPublishKeepsFailing_ShouldDiscardBatchAndShutdownGracefully()
        {
            var options = CreateOptions();
            options.BatchMaxBytes = 1;
            options.PublishRetryCount = 2;
            var publisher = new FailingKafkaLogBatchPublisher(failuresBeforeSuccess: int.MaxValue);
            var client = new KafkaLogClient(options, publisher);

            await client.SendLogAsync(CreatePayload());

            await client.DisposeAsync();
            Assert.Equal(3, publisher.AttemptCount);
        }

        [Fact]
        public void Deserialize_WhenPayloadIsNullLiteral_ShouldThrowJsonException()
        {
            var serializer = new KafkaLogBatchSerializer();

            Assert.Throws<JsonException>(() => serializer.Deserialize("null"));
        }

        [Fact]
        public async Task Dispose_ShouldFlushBufferedLogsBeforeShutdown()
        {
            var options = CreateOptions();
            options.BatchMaxDelayMilliseconds = 60000;
            var publisher = new CapturingKafkaLogBatchPublisher();
            var client = new KafkaLogClient(options, publisher);

            await client.SendLogAsync(CreatePayload());
            client.Dispose();

            Assert.Single(publisher.Batches);
            Assert.Single(publisher.Batches.Single().Events);
        }

        [Fact]
        public async Task SendLogAsync_WhenPublishFailsOnceWithinRetryLimit_ShouldRetryAndSucceed()
        {
            var options = CreateOptions();
            options.BatchMaxBytes = 1;
            options.PublishRetryCount = 1;
            var publisher = new FailingKafkaLogBatchPublisher(failuresBeforeSuccess: 1);
            await using var client = new KafkaLogClient(options, publisher);

            await client.SendLogAsync(CreatePayload());

            var batch = await publisher.WaitForBatchAsync();

            Assert.Equal(2, publisher.AttemptCount);
            Assert.Single(batch.Events);
        }

        private sealed class CapturingKafkaLogBatchPublisher : IKafkaLogBatchPublisher
        {
            private readonly TaskCompletionSource<KafkaLogBatch> _published = new(
                TaskCreationOptions.RunContinuationsAsynchronously
            );

            public ConcurrentQueue<KafkaLogBatch> Batches { get; } = new();

            public Task PublishAsync(KafkaLogBatch batch, CancellationToken cancellationToken)
            {
                Batches.Enqueue(batch);
                _published.TrySetResult(batch);
                return Task.CompletedTask;
            }

            public async Task<KafkaLogBatch> WaitForBatchAsync()
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                return await _published.Task.WaitAsync(cts.Token);
            }

            public void Dispose() { }
        }

        private sealed class FailingKafkaLogBatchPublisher : IKafkaLogBatchPublisher
        {
            private readonly int _failuresBeforeSuccess;
            private readonly TaskCompletionSource<KafkaLogBatch> _published = new(
                TaskCreationOptions.RunContinuationsAsynchronously
            );

            public FailingKafkaLogBatchPublisher(int failuresBeforeSuccess)
            {
                _failuresBeforeSuccess = failuresBeforeSuccess;
            }

            public int AttemptCount { get; private set; }

            public Task PublishAsync(KafkaLogBatch batch, CancellationToken cancellationToken)
            {
                AttemptCount++;
                if (AttemptCount <= _failuresBeforeSuccess)
                {
                    return Task.FromException(new InvalidOperationException("publish failed"));
                }

                _published.TrySetResult(batch);
                return Task.CompletedTask;
            }

            public async Task<KafkaLogBatch> WaitForBatchAsync()
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                return await _published.Task.WaitAsync(cts.Token);
            }

            public void Dispose() { }
        }
    }
}
