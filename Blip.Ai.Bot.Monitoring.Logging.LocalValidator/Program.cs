using Blip.Ai.Bot.Monitoring.Logging.LocalValidator.Diagnostics;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Blip.Ai.Bot.Monitoring.Logging.Services;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;
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
Console.WriteLine();
Console.WriteLine("Sending one event per log category...");

await using (var blipMonitoringLogger = new BlipMonitoringLogger(loggingOptions, logger: Log.Logger))
{
    var mpTrace = BuildInputTrace(LogTitles.Flow.InputProcessing);
    blipMonitoringLogger.MessageProcessing(
        CreateStateExecutionLog(
            LogTitles.Flow.InputProcessing,
            Fixtures.StateId,
            Fixtures.UserAddress,
            new JObject
            {
                ["stateId"] = Fixtures.StateId,
                ["flowId"] = Fixtures.FlowId,
                ["input"] = Fixtures.UserInputText,
                ["inputExecutionTime"] = mpTrace.ElapsedMilliseconds,
                ["error"] = JValue.CreateNull(),
                ["inputTrace"] = JToken.FromObject(mpTrace),
                ["traceSettings"] = BuildTraceSettings(),
            }
        )
    );

    var aeTrace = BuildInputTrace(LogTitles.Flow.ActionExecution);
    blipMonitoringLogger.ActionExecution(
        CreateStateExecutionLog(
            LogTitles.Flow.ActionExecution,
            Fixtures.StateId,
            Fixtures.UserAddress,
            new JObject
            {
                ["stateId"] = Fixtures.StateId,
                ["flowId"] = Fixtures.FlowId,
                ["input"] = Fixtures.UserInputText,
                ["inputExecutionTime"] = aeTrace.ElapsedMilliseconds,
                ["error"] = JValue.CreateNull(),
                ["inputTrace"] = JToken.FromObject(aeTrace),
                ["traceSettings"] = BuildTraceSettings(),
            }
        )
    );

    var ucTrace = BuildInputTrace(LogTitles.Flow.UserContext);
    blipMonitoringLogger.UserContext(
        CreateStateExecutionLog(
            LogTitles.Flow.UserContext,
            Fixtures.StateId,
            Fixtures.UserAddress,
            new JObject
            {
                ["stateId"] = Fixtures.StateId,
                ["flowId"] = Fixtures.FlowId,
                ["input"] = Fixtures.UserInputText,
                ["inputExecutionTime"] = ucTrace.ElapsedMilliseconds,
                ["error"] = JValue.CreateNull(),
                ["inputTrace"] = JToken.FromObject(ucTrace),
                ["traceSettings"] = BuildTraceSettings(),
            }
        )
    );

    var cfTrace = BuildInputTrace(LogTitles.Flow.ConversationalFlow);
    blipMonitoringLogger.ConversationalFlow(
        CreateStateExecutionLog(
            LogTitles.Flow.ConversationalFlow,
            Fixtures.StateId,
            Fixtures.UserAddress,
            new JObject
            {
                ["stateId"] = Fixtures.StateId,
                ["flowId"] = Fixtures.FlowId,
                ["input"] = Fixtures.UserInputText,
                ["inputExecutionTime"] = cfTrace.ElapsedMilliseconds,
                ["error"] = JValue.CreateNull(),
                ["inputTrace"] = JToken.FromObject(cfTrace),
                ["traceSettings"] = BuildTraceSettings(),
            }
        )
    );

    var uiTrace = BuildInputTrace(LogTitles.Flow.UserInput);
    blipMonitoringLogger.UserInput(
        CreateStateExecutionLog(
            LogTitles.Flow.UserInput,
            Fixtures.StateId,
            Fixtures.UserAddress,
            new JObject
            {
                ["stateId"] = Fixtures.StateId,
                ["flowId"] = Fixtures.FlowId,
                ["input"] = Fixtures.UserInputText,
                ["inputExecutionTime"] = uiTrace.ElapsedMilliseconds,
                ["error"] = JValue.CreateNull(),
                ["inputTrace"] = JToken.FromObject(uiTrace),
                ["traceSettings"] = BuildTraceSettings(),
            }
        )
    );

    var mdTrace = BuildInputTrace(LogTitles.Flow.MessageDelivery);
    blipMonitoringLogger.MessageDelivery(
        CreateStateExecutionLog(
            LogTitles.Flow.MessageDelivery,
            Fixtures.StateId,
            Fixtures.UserAddress,
            new JObject
            {
                ["stateId"] = Fixtures.StateId,
                ["flowId"] = Fixtures.FlowId,
                ["input"] = Fixtures.UserInputText,
                ["inputExecutionTime"] = mdTrace.ElapsedMilliseconds,
                ["error"] = JValue.CreateNull(),
                ["inputTrace"] = JToken.FromObject(mdTrace),
                ["traceSettings"] = BuildTraceSettings(),
            }
        )
    );

    var miTrace = BuildInputTrace(LogTitles.Flow.MissingInfoLatency);
    blipMonitoringLogger.MissingInfoLatency(
        CreateStateExecutionLog(
            LogTitles.Flow.MissingInfoLatency,
            Fixtures.StateId,
            Fixtures.UserAddress,
            new JObject
            {
                ["stateId"] = Fixtures.StateId,
                ["flowId"] = Fixtures.FlowId,
                ["input"] = Fixtures.UserInputText,
                ["inputExecutionTime"] = miTrace.ElapsedMilliseconds,
                ["error"] = JValue.CreateNull(),
                ["inputTrace"] = JToken.FromObject(miTrace),
                ["traceSettings"] = BuildTraceSettings(),
            }
        )
    );

    var eeTrace = BuildInputTrace(LogTitles.Flow.ErrorEvents);
    blipMonitoringLogger.ErrorEvents(
        CreateStateExecutionLog(
            LogTitles.Flow.ErrorEvents,
            Fixtures.StateId,
            Fixtures.UserAddress,
            new JObject
            {
                ["stateId"] = Fixtures.StateId,
                ["flowId"] = Fixtures.FlowId,
                ["input"] = Fixtures.UserInputText,
                ["inputExecutionTime"] = eeTrace.ElapsedMilliseconds,
                ["error"] = "Mocked exception for local validation.",
                ["inputTrace"] = JToken.FromObject(eeTrace),
                ["traceSettings"] = BuildTraceSettings(),
            }
        ),
        new InvalidOperationException("Mocked exception for local validation.")
    );

    Console.WriteLine("All events sent. Draining Kafka batches...");
}

Console.WriteLine("Done.");
await Log.CloseAndFlushAsync();

static LogInput CreateStateExecutionLog(
    string title,
    string? stateId,
    string context,
    JObject data
) =>
    new()
    {
        FlowId = Fixtures.FlowId,
        FlowVersion = Fixtures.FlowVersion,
        Title = title,
        IdMessage = "ai-agent-" + Guid.NewGuid(),
        From = context,
        OriginalFrom = context,
        To = Fixtures.BotAddress,
        OriginalTo = Fixtures.BotAddress,
        Operation = string.Empty,
        EventType = "StateExecution",
        StateId = stateId ?? string.Empty,
        Channel = Fixtures.Channel,
        Data = data,
    };

static StateTrace BuildInputTrace(string stateName)
{
    var trace = new StateTrace
    {
        StateId = Guid.NewGuid().ToString(),
        StateName = stateName,
        Input = Fixtures.UserInputText,
        ElapsedMilliseconds = 120,
    };

    trace.InputActions.Add(
        new ActionTrace
        {
            Order = 1,
            Type = "TrackEventAction",
            ActionId = "action-001",
            ActionTitle = "Track user input",
            ContinueOnError = false,
            ElapsedMilliseconds = 38,
            ParsedSettings = new JRaw("""{ "category": "input", "action": "received" }"""),
        }
    );

    trace.InputActions.Add(
        new ActionTrace
        {
            Order = 2,
            Type = "ExecuteScriptAction",
            ActionId = "action-002",
            ActionTitle = "Validate input",
            ContinueOnError = true,
            ElapsedMilliseconds = 21,
            ParsedSettings = new JRaw(
                """{ "function": "validateInput", "inputVariables": ["input"] }"""
            ),
        }
    );

    trace.OutputActions.Add(
        new ActionTrace
        {
            Order = 1,
            Type = "SetVariableAction",
            ActionId = "action-003",
            ActionTitle = "Set next state variable",
            ContinueOnError = false,
            ElapsedMilliseconds = 4,
            ParsedSettings = new JRaw(
                """{ "variable": "nextState", "value": "MenuState" }"""
            ),
        }
    );

    trace.Outputs.Add(
        new OutputTrace
        {
            StateId = "menu-state-id",
            StateName = "MenuState",
            Input = Fixtures.UserInputText,
            ConditionIndex = 0,
            ElapsedMilliseconds = 6,
        }
    );

    return trace;
}

static JObject BuildTraceSettings() =>
    new()
    {
        ["mode"] = "All",
        ["targetType"] = "Lime",
        ["target"] = Fixtures.TraceTarget,
    };

internal static class LogTitles
{
    internal static class Flow
    {
        public const string InputProcessing = "InputProcessing";
        public const string ActionExecution = "ActionExecution";
        public const string UserContext = "UserContext";
        public const string ConversationalFlow = "ConversationalFlow";
        public const string UserInput = "UserInput";
        public const string MessageDelivery = "MessageDelivery";
        public const string MissingInfoLatency = "MissingInfoLatency";
        public const string ErrorEvents = "ErrorEvents";
    }
}

internal static class Fixtures
{
    public const string FlowId = "f0e3257c-c4e1-46d1-b319-9d0757c67af8";
    public const int FlowVersion = 1;
    public const string StateId = "7f7adca5-4614-40b8-a531-08155485929f";
    public const string UserInputText = "hello world";
    public const string BotAddress = "testeanalises@msging.net";
    public const string UserAddress =
        "318f9ad4-ba49-4d47-b79b-d396aeb3f5ba.testeanalises@0mn.io";
    public const string Channel = "0mn.io";
    public const string TraceTarget =
        "318f9ad4-ba49-4d47-b79b-d396aeb3f5ba@tunnel.msging.net";
}
