using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;
using System.Runtime.CompilerServices;
using LogEntry = Blip.Ai.Bot.Monitoring.Logging.Models.Logging;

namespace Blip.Ai.Bot.Monitoring.Logging.Services
{
    public class BlipMonitoringLogger : IBlipLogger
    {
        private const string UNTITLED_LOG = "Untitled log";
        private const string HOST_SERVICE_NAME = "HostServiceName";
        private const int DEFAULT_BATCH_POSTING_LIMIT = 1000;
        private readonly ILogger Logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="BlipMonitoringLogger"/> class with the specified options.
        /// </summary>
        /// <param name="options">The logging options for configuring Serilog sinks.</param>
        public BlipMonitoringLogger(LoggingOptions options)
        {
            var loggerConfig = CreateBaseLoggerConfiguration(options);
            ConfigureSeqSink(loggerConfig, options.Serilog);
            ConfigureConsoleErrorSink(loggerConfig);

            Log.Logger = loggerConfig.CreateLogger();
            Logger = Log.Logger;
        }

        private static LoggerConfiguration CreateBaseLoggerConfiguration(LoggingOptions options)
        {
            return new LoggerConfiguration()
                .Enrich.FromLogContext()
                .Enrich.WithMachineName()
                .Enrich.WithProperty(HOST_SERVICE_NAME, options.HostServiceName!)
                .WriteTo.Console(new RenderedCompactJsonFormatter());
        }

        private static void ConfigureSeqSink(LoggerConfiguration config, SerilogOptions? serilogOptions)
        {
            if (serilogOptions is null)
            {
                return;
            }

            config.WriteTo.Seq(
                serverUrl: serilogOptions.Url,
                batchPostingLimit: DEFAULT_BATCH_POSTING_LIMIT,
                apiKey: serilogOptions.ApiKey
            );
        }

        private static void ConfigureConsoleErrorSink(LoggerConfiguration config)
        {
            config.WriteTo.Console(
                new RenderedCompactJsonFormatter(),
                standardErrorFromLevel: LogEventLevel.Error
            );
        }

        /// <inheritdoc />
        public void LogMessage(
            LogCategory category,
            LogInput input,
            Exception? exception = null,
            LogEventLevel? levelOverride = null,
            [CallerMemberName] string caller = "")
        {
            var entry = CreateLogEntry(category, input, exception, caller);
            var level = ResolveLogLevel(category, levelOverride);

            Logger
                .ForContext(nameof(entry.FlowId), entry.FlowId)
                .ForContext(nameof(entry.Tag), entry.Tag)
                .ForContext(nameof(entry.TagSource), entry.TagSource)
                .ForContext(nameof(entry.Category), entry.Category.ToString())
                .ForContext(nameof(entry.Title), entry.Title)
                .ForContext(nameof(entry.IdMessage), entry.IdMessage)
                .ForContext(nameof(entry.From), entry.From)
                .ForContext(nameof(entry.To), entry.To)
                .ForContext(nameof(entry.Operation), entry.Operation)
                .Write(level, entry.Title ?? UNTITLED_LOG);
        }

        private static LogEntry CreateLogEntry(
            LogCategory category,
            LogInput input,
            Exception? exception,
            string caller)
        {
            return new LogEntry
            {
                Category = category,
                Title = input.Title,
                IdMessage = input.IdMessage,
                From = input.From,
                To = input.To,
                Operation = input.Operation,
                Data = input.Data,
                Exception = exception?.ToString(),
                TagSource = caller,
            };
        }

        private static LogEventLevel ResolveLogLevel(LogCategory category, LogEventLevel? levelOverride)
        {
            return levelOverride ?? category switch
            {
                LogCategory.ErrorEvents => LogEventLevel.Error,
                _ => LogEventLevel.Information
            };
        }

        /// <inheritdoc />
        public void MessageProcessing(LogInput input) => LogMessage(LogCategory.MessageProcessing, input);

        /// <inheritdoc />
        public void ActionExecution(LogInput input) => LogMessage(LogCategory.ActionExecution, input);

        /// <inheritdoc />
        public void UserContext(LogInput input) => LogMessage(LogCategory.UserContext, input);

        /// <inheritdoc />
        public void ConversationalFlow(LogInput input) => LogMessage(LogCategory.ConversationalFlow, input);

        /// <inheritdoc />
        public void UserInput(LogInput input) => LogMessage(LogCategory.UserInput, input);

        /// <inheritdoc />
        public void MessageDelivery(LogInput input) => LogMessage(LogCategory.MessageDelivery, input);

        /// <inheritdoc />
        public void MissingInfoLatency(LogInput input) => LogMessage(LogCategory.MissingInfoLatency, input);

        /// <inheritdoc />
        public void ErrorEvents(LogInput input, Exception exception) => LogMessage(LogCategory.ErrorEvents, input, exception);
    }
}
