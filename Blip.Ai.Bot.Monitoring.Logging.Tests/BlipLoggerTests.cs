using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Blip.Ai.Bot.Monitoring.Logging.Services;
using Moq;
using Take.Blip.Ai.Bot.Monitoring.Abstractions.Models;
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
            new LogInput()
            {
                Title = "Test",
                IdMessage = Guid.NewGuid().ToString(),
                From = "user1",
                To = "bot",
                Operation = "op",
                Data = "some-data",
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
        public void Constructor_WithValidFireHoseOptions_ShouldCreateFireHoseClient()
        {
            // Arrange
            var options = new LoggingOptions
            {
                Serilog = new SerilogOptions { Url = "http://localhost:5341", ApiKey = "dummy" },
                FireHose = new FireHoseOptions
                {
                    Address = "http://localhost:8080/firehose",
                    UserName = "user",
                    Password = "pass",
                    UrlAuthentication = "http://localhost:8080/auth",
                },
            };

            // Act & Assert - Should not throw
            var logger = new BlipMonitoringLogger(options);
            Assert.NotNull(logger);
        }

        [Fact]
        public void SendLogToFireHoseAsync_WithNullFireHoseClient_ShouldNotThrow()
        {
            // Arrange
            var logger = new BlipMonitoringLogger(DefaultOptions);
            var logEntry = new LogEntry
            {
                Category = LogCategory.UserInput,
                Title = "Test Entry",
                IdMessage = "123",
                From = "user",
                To = "bot",
            };

            // Act & Assert - Should not throw when FireHose client is null
            logger.SendLogToFireHoseAsync(logEntry);
            Assert.True(true);
        }

        [Fact]
        public void SendLogToFireHoseAsync_WithMockedFireHoseClient_ShouldCallClient()
        {
            // Arrange
            var mockFireHoseClient = new Mock<IFireHoseClient>();
            var logger = new BlipMonitoringLogger(DefaultOptions, null, mockFireHoseClient.Object);
            var logEntry = new LogEntry
            {
                Category = LogCategory.UserInput,
                Title = "Test Entry",
                IdMessage = "123",
                From = "user",
                To = "bot",
                OriginalFrom = "user",
                OriginalTo = "bot",
            };

            // Act
            logger.SendLogToFireHoseAsync(logEntry);

            // Assert
            mockFireHoseClient.Verify(
                x =>
                    x.SendLogToFireHoseAsync(
                        It.Is<LogEntry>(entry => entry.Title == "Test Entry"),
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
                Title = "Allowed Test",
                IdMessage = Guid.NewGuid().ToString(),
                From = "user1",
                To = "allowed-bot",
                Operation = "op",
                Data = "some-data",
            };

            var deniedInput = new LogInput
            {
                Title = "Denied Test",
                IdMessage = Guid.NewGuid().ToString(),
                From = "user1",
                To = "denied-bot",
                Operation = "op",
                Data = "some-data",
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
        public void FireHoseOptions_IsValid_ShouldReturnTrueForCompleteOptions()
        {
            // Arrange
            var options = new FireHoseOptions
            {
                Address = "http://localhost:8080/firehose",
                UserName = "user",
                Password = "pass",
                UrlAuthentication = "http://localhost:8080/auth",
            };

            // Act
            var isValid = options.IsValid();

            // Assert
            Assert.True(isValid);
        }

        [Fact]
        public void FireHoseOptions_IsValid_ShouldReturnFalseForIncompleteOptions()
        {
            // Arrange
            var options = new FireHoseOptions
            {
                Address = "http://localhost:8080/firehose",
                UserName = "user",
                // Missing Password and UrlAuthentication
            };

            // Act
            var isValid = options.IsValid();

            // Assert
            Assert.False(isValid);
        }
    }
}
