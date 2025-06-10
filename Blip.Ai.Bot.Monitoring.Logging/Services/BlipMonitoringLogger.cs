using System.Runtime.CompilerServices;
using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;
using LogEntry = Blip.Ai.Bot.Monitoring.Logging.Models.Logging;

namespace Blip.Ai.Bot.Monitoring.Logging.Services
{
    public class BlipMonitoringLogger : IBlipLogger
    {
        private static readonly string LABEL_CATEOGRY_HOST_SERVICE_NAME = "HostServiceName";
        private static readonly int DEFAULT_BATCH_POSTING_LIMIT = 1000;
        private readonly ILogger Logger;
        private IFireHoseClient? _fireHoseClient;

        public BlipMonitoringLogger(LoggingOptions options)
        {
            var loggerConfig = new LoggerConfiguration()
                .Enrich.FromLogContext()
                .Enrich.WithMachineName()
                .Enrich.WithProperty(LABEL_CATEOGRY_HOST_SERVICE_NAME, options.HostServiceName!)
                .WriteTo.Console(new RenderedCompactJsonFormatter());

            if (options.Serilog != null)
            {
                loggerConfig.WriteTo.Seq(
                    serverUrl: options.Serilog.Url,
                    batchPostingLimit: DEFAULT_BATCH_POSTING_LIMIT,
                    apiKey: options.Serilog.ApiKey
                );
            }

            loggerConfig.WriteTo.Console(
                new RenderedCompactJsonFormatter(),
                standardErrorFromLevel: LogEventLevel.Error
            );

            Serilog.Log.Logger = loggerConfig.CreateLogger();
            Logger = Serilog.Log.Logger;

            if (options.FireHose != null && options.FireHose.IsValid())
            {
                _fireHoseClient = new FireHoseClient(options.FireHose);
            }
        }

        private void Log(
            LogCategory category,
            LogInput input,
            string? ex = null,
            [CallerMemberName] string caller = ""
        )
        {
            var entry = new LogEntry
            {
                Category = category,
                Title = input.Title,
                IdMessage = input.IdMessage,
                From = input.From,
                To = input.To,
                Operation = input.Operation,
                Data = input.Data,
                Ex = ex,
                TagSource = caller,
            };

            var level =
                category == LogCategory.ErrorEvents
                    ? LogEventLevel.Error
                    : LogEventLevel.Information;

            Logger
                .ForContext("FlowId", entry.FlowId)
                .ForContext("Tag", entry.Tag)
                .ForContext("TagSource", entry.TagSource)
                .ForContext("Category", category.ToString())
                .ForContext("Title", entry.Title)
                .ForContext("IdMessage", entry.IdMessage)
                .ForContext("From", entry.From)
                .ForContext("To", entry.To)
                .ForContext("Operation", entry.Operation)
                .Write(level, entry.Title ?? "Untitled log");

            if(_fireHoseClient != null)
            {
                _fireHoseClient.SendLogToFireHoseAsync(entry, CancellationToken.None).GetAwaiter().GetResult();
            }
        }

        public void MessageProcessing(LogInput input) => Log(LogCategory.MessageProcessing, input);

        public void ActionExecution(LogInput input) => Log(LogCategory.ActionExecution, input);

        public void UserContext(LogInput input) => Log(LogCategory.UserContext, input);

        public void ConversationalFlow(LogInput input) =>
            Log(LogCategory.ConversationalFlow, input);

        public void UserInput(LogInput input) => Log(LogCategory.UserInput, input);

        public void MessageDelivery(LogInput input) => Log(LogCategory.MessageDelivery, input);

        public void MissingInfoLatency(LogInput input) =>
            Log(LogCategory.MissingInfoLatency, input);

        public void ErrorEvents(LogInput input, Exception ex) =>
            Log(LogCategory.ErrorEvents, input, ex.ToString());
    }
}
