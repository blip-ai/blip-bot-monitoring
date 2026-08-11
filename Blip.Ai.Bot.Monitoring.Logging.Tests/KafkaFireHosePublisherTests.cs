using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Confluent.Kafka;
using Moq;

namespace Blip.Ai.Bot.Monitoring.Logging.Tests
{
    public class KafkaFireHosePublisherTests
    {
        private static FireHoseOptions BuildOptions(string? topic = "test-topic") =>
            new FireHoseOptions { KafkaBootstrapServers = "localhost:9092", KafkaTopic = topic };

        // -----------------------------------------------------------------------
        // Construction
        // -----------------------------------------------------------------------

        [Fact]
        public void Constructor_WithNullOptions_ShouldThrowArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new KafkaFireHosePublisher(null!, null));
        }

        [Fact]
        public void Constructor_WithNullOrEmptyTopic_ShouldThrowArgumentException()
        {
            var opts = BuildOptions(topic: null);
            Assert.Throws<ArgumentException>(() => new KafkaFireHosePublisher(opts, null));
        }

        // -----------------------------------------------------------------------
        // Publish
        // -----------------------------------------------------------------------

        [Fact]
        public void Publish_ShouldCallProduce_OnUnderlyingProducer()
        {
            // Arrange
            var mockProducer = new Mock<IProducer<Null, string>>();
            using var publisher = new KafkaFireHosePublisher(BuildOptions(), mockProducer.Object);

            // Act
            publisher.Publish(new { Message = "test" });

            // Assert
            mockProducer.Verify(
                p =>
                    p.Produce(
                        "test-topic",
                        It.Is<Message<Null, string>>(m => m.Value.Contains("test")),
                        It.IsAny<Action<DeliveryReport<Null, string>>>()
                    ),
                Times.Once
            );
        }

        [Fact]
        public void Publish_WithNullEntry_ShouldNotCallProduce()
        {
            // Arrange
            var mockProducer = new Mock<IProducer<Null, string>>();
            using var publisher = new KafkaFireHosePublisher(BuildOptions(), mockProducer.Object);

            // Act
            publisher.Publish(null!);

            // Assert
            mockProducer.Verify(
                p =>
                    p.Produce(
                        It.IsAny<string>(),
                        It.IsAny<Message<Null, string>>(),
                        It.IsAny<Action<DeliveryReport<Null, string>>>()
                    ),
                Times.Never
            );
        }

        [Fact]
        public void Publish_ShouldBeNonBlocking()
        {
            // Arrange
            var mockProducer = new Mock<IProducer<Null, string>>();
            using var publisher = new KafkaFireHosePublisher(BuildOptions(), mockProducer.Object);

            // Act
            var sw = System.Diagnostics.Stopwatch.StartNew();
            publisher.Publish(new { Message = "non-blocking" });
            sw.Stop();

            // Assert - fire-and-forget must return well under 100 ms
            Assert.True(sw.ElapsedMilliseconds < 100);
        }

        [Fact]
        public void Publish_WhenProduceThrowsProduceException_ShouldNotThrow()
        {
            // Arrange
            var mockProducer = new Mock<IProducer<Null, string>>();
            mockProducer
                .Setup(p =>
                    p.Produce(
                        It.IsAny<string>(),
                        It.IsAny<Message<Null, string>>(),
                        It.IsAny<Action<DeliveryReport<Null, string>>>()
                    )
                )
                .Throws(
                    new ProduceException<Null, string>(
                        new Error(ErrorCode.Local_QueueFull),
                        new DeliveryResult<Null, string>()
                    )
                );

            using var publisher = new KafkaFireHosePublisher(BuildOptions(), mockProducer.Object);

            // Act & Assert
            var exception = Record.Exception(() => publisher.Publish(new { Message = "fail" }));
            Assert.Null(exception);
        }

        // -----------------------------------------------------------------------
        // Dispose
        // -----------------------------------------------------------------------

        [Fact]
        public void Dispose_ShouldCallFlush_OnUnderlyingProducer()
        {
            // Arrange
            var mockProducer = new Mock<IProducer<Null, string>>();
            var publisher = new KafkaFireHosePublisher(BuildOptions(), mockProducer.Object);

            // Act
            publisher.Dispose();

            // Assert
            mockProducer.Verify(
                p => p.Flush(It.Is<TimeSpan>(t => t == TimeSpan.FromSeconds(5))),
                Times.Once
            );
        }

        [Fact]
        public void Dispose_ShouldCallDispose_OnUnderlyingProducer()
        {
            // Arrange
            var mockProducer = new Mock<IProducer<Null, string>>();
            var publisher = new KafkaFireHosePublisher(BuildOptions(), mockProducer.Object);

            // Act
            publisher.Dispose();

            // Assert
            mockProducer.Verify(p => p.Dispose(), Times.Once);
        }
    }
}
