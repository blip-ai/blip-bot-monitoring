using Blip.Ai.Bot.Monitoring.Logging.Models;
using Blip.Ai.Bot.Monitoring.Logging.Services;

namespace Blip.Ai.Bot.Monitoring.Logging.Tests;

public class BlipLoggerTests
{
    private static LoggingOptions DefaultOptions =>
        new()
        {
            Serilog = new SerilogOptions { Url = "http://localhost:5341", ApiKey = "dummy" },
        };

    private static LogInput SampleInput =>
        new()
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
    }

    [Fact]
    public void ActionExecution_ShouldExecuteWithoutException()
    {
        var logger = new BlipMonitoringLogger(DefaultOptions);
        logger.ActionExecution(SampleInput);
    }

    [Fact]
    public void UserContext_ShouldExecuteWithoutException()
    {
        var logger = new BlipMonitoringLogger(DefaultOptions);
        logger.UserContext(SampleInput);
    }

    [Fact]
    public void ConversationalFlow_ShouldExecuteWithoutException()
    {
        var logger = new BlipMonitoringLogger(DefaultOptions);
        logger.ConversationalFlow(SampleInput);
    }

    [Fact]
    public void UserInput_ShouldExecuteWithoutException()
    {
        var logger = new BlipMonitoringLogger(DefaultOptions);
        logger.UserInput(SampleInput);
    }

    [Fact]
    public void MessageDelivery_ShouldExecuteWithoutException()
    {
        var logger = new BlipMonitoringLogger(DefaultOptions);
        logger.MessageDelivery(SampleInput);
    }

    [Fact]
    public void MissingInfoLatency_ShouldExecuteWithoutException()
    {
        var logger = new BlipMonitoringLogger(DefaultOptions);
        logger.MissingInfoLatency(SampleInput);
    }

    [Fact]
    public void ErrorEvents_ShouldExecuteWithoutException()
    {
        var logger = new BlipMonitoringLogger(DefaultOptions);
        var ex = new InvalidOperationException("dummy error");
        logger.ErrorEvents(SampleInput, ex);
    }
}
