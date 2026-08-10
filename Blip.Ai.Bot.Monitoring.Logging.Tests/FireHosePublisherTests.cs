using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Moq;

namespace Blip.Ai.Bot.Monitoring.Logging.Tests
{
    public class FireHosePublisherTests
    {
        private static FireHoseOptions FastOptions =>
            new FireHoseOptions
            {
                ChannelCapacity = 10_000,
                BatchSize = 100,
                FlushIntervalMs = 50,
            };

        [Fact]
        public void Publish_ShouldEnqueueEntry_WithoutBlocking()
        {
            // Arrange
            var mockClient = new Mock<IFireHoseClient>();
            mockClient
                .Setup(c =>
                    c.SendBatchToFireHoseAsync(
                        It.IsAny<IReadOnlyList<object>>(),
                        It.IsAny<CancellationToken>()
                    )
                )
                .Returns(Task.CompletedTask);

            using var publisher = new FireHosePublisher(mockClient.Object, FastOptions);

            // Act - Should return immediately without blocking
            var sw = System.Diagnostics.Stopwatch.StartNew();
            publisher.Publish(new { Message = "test" });
            sw.Stop();

            // Assert - Publish is non-blocking, should complete well under 100ms
            Assert.True(sw.ElapsedMilliseconds < 100);
        }

        [Fact]
        public void Publish_WhenChannelFull_ShouldDropOldestWithoutThrowing()
        {
            // Arrange
            var mockClient = new Mock<IFireHoseClient>();
            mockClient
                .Setup(c =>
                    c.SendBatchToFireHoseAsync(
                        It.IsAny<IReadOnlyList<object>>(),
                        It.IsAny<CancellationToken>()
                    )
                )
                .Returns(Task.Delay(10_000)); // block the background worker

            var tinyOptions = new FireHoseOptions
            {
                ChannelCapacity = 5,
                BatchSize = 100,
                FlushIntervalMs = 50,
            };

            using var publisher = new FireHosePublisher(mockClient.Object, tinyOptions);

            // Act - Overfill the channel; should not throw
            var exception = Record.Exception(() =>
            {
                for (var i = 0; i < 20; i++)
                    publisher.Publish(new { Index = i });
            });

            // Assert
            Assert.Null(exception);
        }

        [Fact]
        public async Task Publish_ShouldFlushBatchAfterFlushInterval()
        {
            // Arrange
            var flushedBatches = new List<IReadOnlyList<object>>();
            var tcs = new TaskCompletionSource<bool>();

            var mockClient = new Mock<IFireHoseClient>();
            mockClient
                .Setup(c =>
                    c.SendBatchToFireHoseAsync(
                        It.IsAny<IReadOnlyList<object>>(),
                        It.IsAny<CancellationToken>()
                    )
                )
                .Callback<IReadOnlyList<object>, CancellationToken>(
                    (batch, _) =>
                    {
                        flushedBatches.Add(batch);
                        tcs.TrySetResult(true);
                    }
                )
                .Returns(Task.CompletedTask);

            using var publisher = new FireHosePublisher(mockClient.Object, FastOptions);

            // Act - Publish a single entry (less than batch size) and wait for flush interval
            publisher.Publish(new { Message = "flush-test" });

            // Wait up to 2 seconds for the flush to happen
            var completed = await Task.WhenAny(tcs.Task, Task.Delay(2000));

            // Assert
            Assert.True(
                completed == tcs.Task,
                "Batch was not flushed within the expected time window."
            );
            Assert.NotEmpty(flushedBatches);
            Assert.Single(flushedBatches[0]);
        }

        [Fact]
        public async Task Publish_ShouldFlushBatchWhenBatchSizeReached()
        {
            // Arrange
            var flushedBatches = new List<IReadOnlyList<object>>();
            var tcs = new TaskCompletionSource<bool>();

            var mockClient = new Mock<IFireHoseClient>();
            mockClient
                .Setup(c =>
                    c.SendBatchToFireHoseAsync(
                        It.IsAny<IReadOnlyList<object>>(),
                        It.IsAny<CancellationToken>()
                    )
                )
                .Callback<IReadOnlyList<object>, CancellationToken>(
                    (batch, _) =>
                    {
                        flushedBatches.Add(batch);
                        tcs.TrySetResult(true);
                    }
                )
                .Returns(Task.CompletedTask);

            var options = new FireHoseOptions
            {
                ChannelCapacity = 10_000,
                BatchSize = 5,
                FlushIntervalMs = 5_000, // long interval so only batch-size triggers flush
            };

            using var publisher = new FireHosePublisher(mockClient.Object, options);

            // Act - publish exactly BatchSize entries
            for (var i = 0; i < 5; i++)
                publisher.Publish(new { Index = i });

            // Wait up to 2 seconds for the flush to happen
            var completed = await Task.WhenAny(tcs.Task, Task.Delay(2000));

            // Assert
            Assert.True(
                completed == tcs.Task,
                "Batch was not flushed within the expected time window."
            );
            Assert.NotEmpty(flushedBatches);
            Assert.Equal(5, flushedBatches[0].Count);
        }

        [Fact]
        public async Task Dispose_ShouldDrainRemainingEntries()
        {
            // Arrange
            var totalReceived = 0;
            var mockClient = new Mock<IFireHoseClient>();
            mockClient
                .Setup(c =>
                    c.SendBatchToFireHoseAsync(
                        It.IsAny<IReadOnlyList<object>>(),
                        It.IsAny<CancellationToken>()
                    )
                )
                .Callback<IReadOnlyList<object>, CancellationToken>(
                    (batch, _) => totalReceived += batch.Count
                )
                .Returns(Task.CompletedTask);

            var publisher = new FireHosePublisher(mockClient.Object, FastOptions);

            // Act - publish some entries then dispose
            for (var i = 0; i < 3; i++)
                publisher.Publish(new { Index = i });

            // Small delay to let background worker start
            await Task.Delay(10);

            publisher.Dispose(); // Should drain remaining entries

            // Assert - all entries should have been sent
            Assert.Equal(3, totalReceived);
        }

        [Fact]
        public void Constructor_WithNullClient_ShouldThrowArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new FireHosePublisher(null!));
        }

        [Fact]
        public void Publish_WithNullEntry_ShouldNotThrow()
        {
            // Arrange
            var mockClient = new Mock<IFireHoseClient>();
            using var publisher = new FireHosePublisher(mockClient.Object, FastOptions);

            // Act & Assert
            var exception = Record.Exception(() => publisher.Publish(null!));
            Assert.Null(exception);
        }
    }
}
