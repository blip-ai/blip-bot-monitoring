using Blip.Ai.Bot.Monitoring.Logging.Models;
using Blip.Ai.Bot.Monitoring.Logging.Services;
using Microsoft.Extensions.Configuration;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console(
        outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}"
    )
    .CreateLogger();

var configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
    .AddJsonFile("appsettings.Local.json", optional: true, reloadOnChange: false)
    .Build();

var loggingOptions =
    configuration.GetSection("Logging").Get<LoggingOptions>()
    ?? throw new InvalidOperationException("Missing 'Logging' configuration section.");

Console.WriteLine("Blip Monitoring Logger - Local Validator");
Console.WriteLine($"Kafka topic: {loggingOptions.Kafka?.Topic ?? "(not configured)"}");

await using (var logger = new BlipMonitoringLogger(loggingOptions, logger: Log.Logger))
{
    logger.MessageProcessing(MakeInput("MessageProcessing"));
    logger.ActionExecution(MakeInput("ActionExecution"));
    logger.UserContext(MakeInput("UserContext"));
    logger.ConversationalFlow(MakeInput("ConversationalFlow"));
    logger.UserInput(MakeInput("UserInput"));
    logger.MessageDelivery(MakeInput("MessageDelivery"));
    logger.MissingInfoLatency(MakeInput("MissingInfoLatency"));
    logger.ErrorEvents(
        MakeInput("ErrorEvents"),
        new InvalidOperationException("Mocked exception for local validation.")
    );

    Console.WriteLine("All events sent. Draining Kafka batches...");
}

Console.WriteLine("Validation complete.");
await Log.CloseAndFlushAsync();

static LogInput MakeInput(string title) =>
    new()
    {
        FlowId = "flow-local-validator-001",
        Title = title,
        IdMessage = Guid.NewGuid().ToString(),
        From = "user@local.validator",
        OriginalFrom = "user@local.validator",
        To = "bot@local.validator",
        OriginalTo = "bot@local.validator",
        Operation = "send",
        EventType = "message",
        StateId = "state-init",
        Channel = "local",
        FlowVersion = 1,
        Data = new { text = $"Mock event: {title}", timestamp = DateTimeOffset.UtcNow },
    };
