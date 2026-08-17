using System.Collections.Concurrent;
using System.Text.Json;
using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Models;
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
                SaslPassword = "password",
                SaslUsername = "username",
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

        [Fact]
        public async Task SendLogAsync_MultiplePayloads_ShouldAccumulateInSingleBatch()
        {
            var options = CreateOptions();
            options.BatchMaxBytes = 1024 * 1024;
            options.BatchMaxDelayMilliseconds = 50;
            var publisher = new CapturingKafkaLogBatchPublisher();
            await using var client = new KafkaLogClient(options, publisher);

            await client.SendLogAsync(CreatePayload("ev1"));
            await client.SendLogAsync(CreatePayload("ev2"));
            await client.SendLogAsync(CreatePayload("ev3"));

            var batch = await publisher.WaitForBatchAsync();
            Assert.Equal(3, batch.Events.Length);
        }

        [Fact]
        public async Task SendLogAsync_PayloadsExceedingBatchMaxBytes_ShouldPublishMultipleBatches()
        {
            var options = CreateOptions();
            options.BatchMaxBytes = 1;
            var publisher = new CapturingKafkaLogBatchPublisher(expectedBatchCount: 3);
            await using var client = new KafkaLogClient(options, publisher);

            await client.SendLogAsync(CreatePayload("ev1"));
            await client.SendLogAsync(CreatePayload("ev2"));
            await client.SendLogAsync(CreatePayload("ev3"));

            await publisher.WaitForAllBatchesAsync(expectedCount: 3);
            Assert.Equal(3, publisher.Batches.Count);
        }

        [Fact]
        public async Task SendLogAsync_AfterDispose_ShouldThrowObjectDisposedException()
        {
            var options = CreateOptions();
            var publisher = new CapturingKafkaLogBatchPublisher();
            var client = new KafkaLogClient(options, publisher);
            await client.DisposeAsync();

            await Assert.ThrowsAsync<ObjectDisposedException>(() =>
                client.SendLogAsync(CreatePayload())
            );
        }

        [Fact]
        public async Task SendLogAsync_EventBytes_ShouldBeValidJson()
        {
            var options = CreateOptions();
            options.BatchMaxBytes = 1;
            var publisher = new CapturingKafkaLogBatchPublisher();
            await using var client = new KafkaLogClient(options, publisher);

            var payload = CreatePayload("json-check");
            await client.SendLogAsync(payload);

            var batch = await publisher.WaitForBatchAsync();
            Assert.Single(batch.Events);

            using var doc = System.Text.Json.JsonDocument.Parse(batch.Events[0]);
            Assert.Equal("json-check", doc.RootElement.GetProperty("Title").GetString());
        }

        [Fact]
        public async Task DisposeAsync_WithNoPendingLogs_ShouldCompleteImmediately()
        {
            var options = CreateOptions();
            var publisher = new CapturingKafkaLogBatchPublisher();
            var client = new KafkaLogClient(options, publisher);

            await client.DisposeAsync();

            Assert.Empty(publisher.Batches);
        }

        [Fact]
        public async Task DisposeAsync_CalledTwice_ShouldNotThrow()
        {
            var options = CreateOptions();
            var publisher = new CapturingKafkaLogBatchPublisher();
            var client = new KafkaLogClient(options, publisher);

            await client.DisposeAsync();
            await client.DisposeAsync();
        }

        [Fact]
        public void Dispose_CalledTwice_ShouldNotThrow()
        {
            var options = CreateOptions();
            var publisher = new CapturingKafkaLogBatchPublisher();
            var client = new KafkaLogClient(options, publisher);

            client.Dispose();
            client.Dispose();
        }

        [Fact]
        public async Task SendLogAsync_WhenRetryCountIsZero_ShouldNotRetryOnFailure()
        {
            var options = CreateOptions();
            options.BatchMaxBytes = 1;
            options.PublishRetryCount = 0;
            var publisher = new FailingKafkaLogBatchPublisher(failuresBeforeSuccess: int.MaxValue);
            await using var client = new KafkaLogClient(options, publisher);

            await client.SendLogAsync(CreatePayload());

            await Task.Delay(200);
            Assert.Equal(1, publisher.AttemptCount);
        }

        private sealed class CapturingKafkaLogBatchPublisher : IKafkaLogBatchPublisher
        {
            private readonly TaskCompletionSource<KafkaLogBatch> _firstPublished = new(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            private readonly int _expectedBatchCount;
            private TaskCompletionSource<bool>? _allReceived;

            public CapturingKafkaLogBatchPublisher(int expectedBatchCount = 1)
            {
                _expectedBatchCount = expectedBatchCount;
                if (expectedBatchCount > 1)
                {
                    _allReceived = new TaskCompletionSource<bool>(
                        TaskCreationOptions.RunContinuationsAsynchronously
                    );
                }
            }

            public ConcurrentQueue<KafkaLogBatch> Batches { get; } = new();

            public Task PublishAsync(KafkaLogBatch batch, CancellationToken cancellationToken)
            {
                Batches.Enqueue(batch);
                _firstPublished.TrySetResult(batch);
                if (_allReceived != null && Batches.Count >= _expectedBatchCount)
                {
                    _allReceived.TrySetResult(true);
                }
                return Task.CompletedTask;
            }

            public async Task<KafkaLogBatch> WaitForBatchAsync()
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                return await _firstPublished.Task.WaitAsync(cts.Token);
            }

            public async Task WaitForAllBatchesAsync(int expectedCount)
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                var waitTask = _allReceived != null ? (Task)_allReceived.Task : Task.CompletedTask;
                await waitTask.WaitAsync(cts.Token);
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
