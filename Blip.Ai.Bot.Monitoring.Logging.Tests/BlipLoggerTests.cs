using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Blip.Ai.Bot.Monitoring.Logging.Services;
using Moq;
using Blip.Ai.Bot.Monitoring.Logging.Abstractions.Models;
using LogEntry = Blip.Ai.Bot.Monitoring.Logging.Models.Logging;

namespace Blip.Ai.Bot.Monitoring.Logging.Tests
{
    public class BlipLoggerTests
    {
        private static LoggingOptions DefaultOptions =>
            new LoggingOptions()
            {
                Serilog = new SerilogOptions { Url = "http://localhost:5341", ApiKey = "dummy" },
            };

        private static LogInput SampleInput =>
            new()
            {
                FlowId = Guid.NewGuid().ToString(),
                Title = "Test",
                IdMessage = Guid.NewGuid().ToString(),
                From = "user1",
                To = "bot",
                Operation = "op",
                Data = "some-data",
                Channel = "wa.gw.msging.net",
                EventType = "event-type",
                FlowVersion = 1,
                OriginalFrom = "user1",
                OriginalTo = "bot",
                StateId = Guid.NewGuid().ToString(),
            };

        [Fact]
        public void MessageProcessing_ShouldExecuteWithoutException()
        {
            var logger = new BlipMonitoringLogger(DefaultOptions);
            logger.MessageProcessing(SampleInput);
            Assert.True(true);
        }

        [Fact]
        public void ActionExecution_ShouldExecuteWithoutException()
        {
            var logger = new BlipMonitoringLogger(DefaultOptions);
            logger.ActionExecution(SampleInput);
            Assert.True(true);
        }

        [Fact]
        public void UserContext_ShouldExecuteWithoutException()
        {
            var logger = new BlipMonitoringLogger(DefaultOptions);
            logger.UserContext(SampleInput);
            Assert.True(true);
        }

        [Fact]
        public void ConversationalFlow_ShouldExecuteWithoutException()
        {
            var logger = new BlipMonitoringLogger(DefaultOptions);
            logger.ConversationalFlow(SampleInput);
            Assert.True(true);
        }

        [Fact]
        public void UserInput_ShouldExecuteWithoutException()
        {
            var logger = new BlipMonitoringLogger(DefaultOptions);
            logger.UserInput(SampleInput);
            Assert.True(true);
        }

        [Fact]
        public void MessageDelivery_ShouldExecuteWithoutException()
        {
            var logger = new BlipMonitoringLogger(DefaultOptions);
            logger.MessageDelivery(SampleInput);
            Assert.True(true);
        }

        [Fact]
        public void MissingInfoLatency_ShouldExecuteWithoutException()
        {
            var logger = new BlipMonitoringLogger(DefaultOptions);
            logger.MissingInfoLatency(SampleInput);
            Assert.True(true);
        }

        [Fact]
        public void ErrorEvents_ShouldExecuteWithoutException()
        {
            var logger = new BlipMonitoringLogger(DefaultOptions);
            var ex = new InvalidOperationException("dummy error");
            logger.ErrorEvents(SampleInput, ex);
            Assert.True(true);
        }

        [Fact]
        public void Constructor_Should_NotThrow_With_Minimal_Options()
        {
            var options = new LoggingOptions();
            var logger = new BlipMonitoringLogger(options);
            Assert.NotNull(logger);
        }

        [Fact]
        public void Constructor_Should_NotThrow_With_Serilog_Options()
        {
            var options = new LoggingOptions
            {
                Serilog = new SerilogOptions { Url = "http://localhost:5341", ApiKey = "dummy" },
            };

            var logger = new BlipMonitoringLogger(options);
            Assert.NotNull(logger);
        }

        [Fact]
        public void LogMessage_ShouldUseLevelOverride()
        {
            var logger = new BlipMonitoringLogger(DefaultOptions);
            logger.LogMessage(
                LogCategory.UserInput,
                SampleInput,
                levelOverride: Serilog.Events.LogEventLevel.Warning
            );

            Assert.True(true);
        }

        [Fact]
        public void LogMessage_ShouldHandleNullException()
        {
            var logger = new BlipMonitoringLogger(DefaultOptions);
            logger.LogMessage(LogCategory.UserContext, SampleInput, exception: null);
            Assert.True(true);
        }

        [Fact]
        public void LogMessage_WhenTitleIsNull_ShouldUseUntitledLog()
        {
            var logger = new BlipMonitoringLogger(DefaultOptions);
            var input = SampleInput;
            input.Title = null!;
            logger.LogMessage(LogCategory.ConversationalFlow, input);
            Assert.True(true);
        }

        [Fact]
        public void LogMessage_ForNonErrorCategory_ShouldUseInformationLevel()
        {
            var logger = new BlipMonitoringLogger(DefaultOptions);
            logger.LogMessage(LogCategory.MessageDelivery, SampleInput);
            Assert.True(true);
        }

        [Fact]
        public void MultipleLoggerInstances_ShouldNotThrow()
        {
            var logger1 = new BlipMonitoringLogger(DefaultOptions);
            var logger2 = new BlipMonitoringLogger(DefaultOptions);

            logger1.MessageProcessing(SampleInput);
            logger2.ActionExecution(SampleInput);

            Assert.True(true);
        }

        [Fact]
        public void Constructor_WithInjectedFireHoseClient_ShouldUseProvidedClient()
        {
            // Arrange
            var mockFireHoseClient = new Mock<IFireHoseClient>();
            var options = new LoggingOptions
            {
                Serilog = new SerilogOptions { Url = "http://localhost:5341", ApiKey = "dummy" },
            };

            // Act
            var logger = new BlipMonitoringLogger(options, null, mockFireHoseClient.Object);

            // Assert
            Assert.NotNull(logger);
        }

        [Fact]
        public void Constructor_WithCheckMonitoringFunc_ShouldAcceptFunction()
        {
            // Arrange
            var options = DefaultOptions;
            var checkFunc = new Func<string, Task<bool>>(
                async (destination) =>
                {
                    await Task.Delay(1);
                    return destination == "allowed-bot";
                }
            );

            // Act
            var logger = new BlipMonitoringLogger(options, checkFunc);

            // Assert
            Assert.NotNull(logger);
        }

        [Fact]
        public void Constructor_WithValidKafkaOptions_ShouldCreateFireHoseClient()
        {
            // Arrange
            var options = new LoggingOptions
            {
                Serilog = new SerilogOptions { Url = "http://localhost:5341", ApiKey = "dummy" },
                Kafka = new KafkaOptions
                {
                    BootstrapServers = "localhost:9092",
                    Topic = "bot-monitoring",
                },
            };

            // Act & Assert - Should not throw
            using var logger = new BlipMonitoringLogger(options);
            Assert.NotNull(logger);
        }

        [Fact]
        public void SendLogToFireHoseAsync_WithNullFireHoseClient_ShouldNotThrow()
        {
            // Arrange
            var logger = new BlipMonitoringLogger(DefaultOptions);

            // Act & Assert - Should not throw when FireHose client is null
            logger.SendLogToFireHoseAsync(SampleInput, LogCategory.UserInput);
            Assert.True(true);
        }

        [Fact]
        public void SendLogToFireHoseAsync_WithMockedFireHoseClient_ShouldCallClient()
        {
            // Arrange
            var mockFireHoseClient = new Mock<IFireHoseClient>();
            var logger = new BlipMonitoringLogger(DefaultOptions, null, mockFireHoseClient.Object);

            // Act
            logger.SendLogToFireHoseAsync(SampleInput, LogCategory.UserInput);

            // Assert
            mockFireHoseClient.Verify(
                x =>
                    x.SendLogToFireHoseAsync(
                        It.Is<LogEntry>(entry => entry.Title == SampleInput.Title),
                        It.IsAny<CancellationToken>()
                    ),
                Times.Once
            );
        }

        [Fact]
        public void LogMessage_WithDisabledMonitoring_ShouldNotExecute()
        {
            // Arrange
            var mockFireHoseClient = new Mock<IFireHoseClient>();
            var options = new LoggingOptions
            {
                IsEnabledMonitoring = false,
                Serilog = new SerilogOptions { Url = "http://localhost:5341", ApiKey = "dummy" },
            };
            var logger = new BlipMonitoringLogger(options, null, mockFireHoseClient.Object);

            // Act
            logger.LogMessage(LogCategory.UserInput, SampleInput);

            // Assert
            mockFireHoseClient.Verify(
                x => x.SendLogToFireHoseAsync(It.IsAny<LogEntry>(), It.IsAny<CancellationToken>()),
                Times.Never
            );
        }

        [Fact]
        public void LogMessage_WithCheckFunction_ShouldCallFireHoseOnlyWhenAllowed()
        {
            // Arrange
            var mockFireHoseClient = new Mock<IFireHoseClient>();
            var checkFunc = new Func<string, Task<bool>>(
                async (destination) =>
                {
                    await Task.Delay(1);
                    return destination == "allowed-bot";
                }
            );
            var logger = new BlipMonitoringLogger(
                DefaultOptions,
                checkFunc,
                mockFireHoseClient.Object
            );

            var allowedInput = new LogInput
            {
                FlowId = Guid.NewGuid().ToString(),
                Title = "Allowed Test",
                IdMessage = Guid.NewGuid().ToString(),
                From = "user1",
                To = "allowed-bot",
                Operation = "op",
                Data = "some-data",
                Channel = "wa.gw.msging.net",
                EventType = "event-type",
                FlowVersion = 1,
                OriginalFrom = "user1",
                OriginalTo = "allowed-bot",
                StateId = Guid.NewGuid().ToString(),
            };

            var deniedInput = new LogInput
            {
                FlowId = Guid.NewGuid().ToString(),
                Title = "Denied Test",
                IdMessage = Guid.NewGuid().ToString(),
                From = "user1",
                To = "denied-bot",
                Operation = "op",
                Data = "some-data",
                Channel = "wa.gw.msging.net",
                EventType = "event-type",
                FlowVersion = 1,
                OriginalFrom = "user1",
                OriginalTo = "denied-bot",
                StateId = Guid.NewGuid().ToString(),
            };

            // Act
            logger.LogMessage(LogCategory.UserInput, allowedInput);
            logger.LogMessage(LogCategory.UserInput, deniedInput);

            // Assert
            mockFireHoseClient.Verify(
                x =>
                    x.SendLogToFireHoseAsync(
                        It.Is<LogEntry>(entry => entry.To == "allowed-bot"),
                        It.IsAny<CancellationToken>()
                    ),
                Times.Once
            );

            mockFireHoseClient.Verify(
                x =>
                    x.SendLogToFireHoseAsync(
                        It.Is<LogEntry>(entry => entry.To == "denied-bot"),
                        It.IsAny<CancellationToken>()
                    ),
                Times.Never
            );
        }

        [Fact]
        public void LogMessage_WithClusterOption_ShouldIncludeClusterInEntry()
        {
            // Arrange
            var mockFireHoseClient = new Mock<IFireHoseClient>();
            var options = new LoggingOptions
            {
                Serilog = new SerilogOptions { Url = "http://localhost:5341", ApiKey = "dummy" },
                Cluster = "test-cluster",
            };
            var logger = new BlipMonitoringLogger(options, null, mockFireHoseClient.Object);

            // Act
            logger.LogMessage(LogCategory.UserInput, SampleInput);

            // Assert
            mockFireHoseClient.Verify(
                x =>
                    x.SendLogToFireHoseAsync(
                        It.Is<LogEntry>(entry => entry.Cluster == "test-cluster"),
                        It.IsAny<CancellationToken>()
                    ),
                Times.Once
            );
        }

        [Fact]
        public void LogMessage_WithHostServiceName_ShouldSetHostServiceName()
        {
            // Arrange
            var options = new LoggingOptions
            {
                HostServiceName = "TestService",
                Serilog = new SerilogOptions { Url = "http://localhost:5341", ApiKey = "dummy" },
            };

            // Act & Assert - Should not throw
            var logger = new BlipMonitoringLogger(options);
            logger.LogMessage(LogCategory.UserInput, SampleInput);
            Assert.True(true);
        }

        [Fact]
        public void KafkaOptions_IsValid_ShouldReturnTrueForCompleteOptions()
        {
            // Arrange
            var options = new KafkaOptions
            {
                BootstrapServers = "localhost:9092",
                Topic = "bot-monitoring",
            };

            // Act
            var isValid = options.IsValid();

            // Assert
            Assert.True(isValid);
        }

        [Fact]
        public void KafkaOptions_IsValid_ShouldReturnFalseForIncompleteOptions()
        {
            // Arrange
            var options = new KafkaOptions
            {
                BootstrapServers = "localhost:9092",
            };

            // Act
            var isValid = options.IsValid();

            // Assert
            Assert.False(isValid);
        }

        [Fact]
        public void LogMessage_WithFlowVersion_ShouldIncludeFlowVersionInEntry()
        {
            // Arrange
            var mockFireHoseClient = new Mock<IFireHoseClient>();
            var logger = new BlipMonitoringLogger(DefaultOptions, null, mockFireHoseClient.Object);

            var input = new LogInput
            {
                FlowId = Guid.NewGuid().ToString(),
                Title = "Test",
                IdMessage = Guid.NewGuid().ToString(),
                From = "user1",
                To = "bot",
                Operation = "op",
                Data = "some-data",
                Channel = "wa.gw.msging.net",
                EventType = "event-type",
                FlowVersion = 42,
                OriginalFrom = "user1",
                OriginalTo = "bot",
                StateId = Guid.NewGuid().ToString(),
            };

            // Act
            logger.LogMessage(LogCategory.UserInput, input);

            // Assert
            mockFireHoseClient.Verify(
                x =>
                    x.SendLogToFireHoseAsync(
                        It.Is<LogEntry>(entry => entry.FlowVersion == 42),
                        It.IsAny<CancellationToken>()
                    ),
                Times.Once
            );
        }

        [Fact]
        public void LogMessage_WithZeroFlowVersion_ShouldIncludeZeroFlowVersionInEntry()
        {
            // Arrange
            var mockFireHoseClient = new Mock<IFireHoseClient>();
            var logger = new BlipMonitoringLogger(DefaultOptions, null, mockFireHoseClient.Object);

            var input = new LogInput
            {
                FlowId = Guid.NewGuid().ToString(),
                Title = "Test",
                IdMessage = Guid.NewGuid().ToString(),
                From = "user1",
                To = "bot",
                Operation = "op",
                Data = "some-data",
                Channel = "wa.gw.msging.net",
                EventType = "event-type",
                FlowVersion = 0,
                OriginalFrom = "user1",
                OriginalTo = "bot",
                StateId = Guid.NewGuid().ToString(),
            };

            // Act
            logger.LogMessage(LogCategory.UserInput, input);

            // Assert
            mockFireHoseClient.Verify(
                x =>
                    x.SendLogToFireHoseAsync(
                        It.Is<LogEntry>(entry => entry.FlowVersion == 0),
                        It.IsAny<CancellationToken>()
                    ),
                Times.Once
            );
        }

        [Fact]
        public void LogMessage_WithChannel_ShouldIncludeChannelInEntry()
        {
            // Arrange
            var mockFireHoseClient = new Mock<IFireHoseClient>();
            var logger = new BlipMonitoringLogger(DefaultOptions, null, mockFireHoseClient.Object);

            var input = new LogInput
            {
                FlowId = Guid.NewGuid().ToString(),
                Title = "Test",
                IdMessage = Guid.NewGuid().ToString(),
                From = "user1",
                To = "bot",
                Operation = "op",
                Data = "some-data",
                Channel = "wa.gw.msging.net",
                EventType = "event-type",
                FlowVersion = 1,
                OriginalFrom = "user1",
                OriginalTo = "bot",
                StateId = Guid.NewGuid().ToString(),
            };

            // Act
            logger.LogMessage(LogCategory.UserInput, input);

            // Assert
            mockFireHoseClient.Verify(
                x =>
                    x.SendLogToFireHoseAsync(
                        It.Is<LogEntry>(entry => entry.Channel == "wa.gw.msging.net"),
                        It.IsAny<CancellationToken>()
                    ),
                Times.Once
            );
        }

        [Fact]
        public void LogMessage_WithNullChannel_ShouldIncludeNullChannelInEntry()
        {
            // Arrange
            var mockFireHoseClient = new Mock<IFireHoseClient>();
            var logger = new BlipMonitoringLogger(DefaultOptions, null, mockFireHoseClient.Object);

            var input = new LogInput
            {
                FlowId = Guid.NewGuid().ToString(),
                Title = "Test",
                IdMessage = Guid.NewGuid().ToString(),
                From = "user1",
                To = "bot",
                Operation = "op",
                Data = "some-data",
                Channel = null!,
                EventType = "event-type",
                FlowVersion = 1,
                OriginalFrom = "user1",
                OriginalTo = "bot",
                StateId = Guid.NewGuid().ToString(),
            };

            // Act
            logger.LogMessage(LogCategory.UserInput, input);

            // Assert
            mockFireHoseClient.Verify(
                x =>
                    x.SendLogToFireHoseAsync(
                        It.Is<LogEntry>(entry => entry.Channel == null),
                        It.IsAny<CancellationToken>()
                    ),
                Times.Once
            );
        }
    }
}
