using System.Runtime.CompilerServices;
using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;
using Take.Blip.Ai.Bot.Monitoring.Abstractions;
using Take.Blip.Ai.Bot.Monitoring.Abstractions.Models;
using LogEntry = Blip.Ai.Bot.Monitoring.Logging.Models.Logging;

namespace Blip.Ai.Bot.Monitoring.Logging.Services
{
    public class BlipMonitoringLogger : IBlipLogger
    {
        private static readonly int DEFAULT_BATCH_POSTING_LIMIT = 1000;
        private const string UNTITLED_LOG = "Untitled log";
        private const string HOST_SERVICE_NAME = "HostServiceName";
        private readonly ILogger Logger;
        private readonly IFireHoseClient? _fireHoseClient;
        private bool _isEnabledMonitoring = true;
        private string _cluster = string.Empty;
        private readonly Func<string, Task<bool>>? _checkIfMonitoringIsRegisteredFuncAsync = null;

        /// <summary>
        /// Initializes a new instance of the <see cref="BlipMonitoringLogger"/> class with the specified options.
        /// </summary>
        /// <param name="options">The logging options for configuring Serilog sinks.</param>
        /// <param name="checkIfMonitoringIsRegisteredFuncAsync">An optional function to determine if monitoring is enabled for a specific destination.</param>
        /// <param name="fireHoseClient">An optional FireHose client for sending logs. If not provided, a new instance will be created if FireHose options are valid.</param>
        public BlipMonitoringLogger(
            LoggingOptions options,
            Func<string, Task<bool>>? checkIfMonitoringIsRegisteredFuncAsync = null,
            IFireHoseClient? fireHoseClient = null
        )
        {
            var loggerConfig = CreateBaseLoggerConfiguration(options);
            ConfigureSeqSink(loggerConfig, options.Serilog);
            ConfigureConsoleErrorSink(loggerConfig);
            _isEnabledMonitoring = options.IsEnabledMonitoring;
            _cluster = options.Cluster ?? string.Empty;

            if (fireHoseClient != null)
            {
                _fireHoseClient = fireHoseClient;
            }
            else if (options.FireHose != null && options.FireHose.IsValid())
            {
                _fireHoseClient = new FireHoseClient(options.FireHose);
            }

            Log.Logger = loggerConfig.CreateLogger();
            Logger = Log.Logger;
            _checkIfMonitoringIsRegisteredFuncAsync = checkIfMonitoringIsRegisteredFuncAsync;
        }

        private static LoggerConfiguration CreateBaseLoggerConfiguration(LoggingOptions options)
        {
            return new LoggerConfiguration()
                .Enrich.FromLogContext()
                .Enrich.WithMachineName()
                .Enrich.WithProperty(HOST_SERVICE_NAME, options.HostServiceName!)
                .WriteTo.Console(new RenderedCompactJsonFormatter());
        }

        private static void ConfigureSeqSink(
            LoggerConfiguration config,
            SerilogOptions? serilogOptions
        )
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
            [CallerMemberName] string caller = ""
        )
        {
            if (!_isEnabledMonitoring)
            {
                return;
            }

            var entry = CreateLogEntry(category, input, exception, caller, _cluster);
            var level = ResolveLogLevel(category, levelOverride);

            Logger
                .ForContext(nameof(entry.FlowId), entry.FlowId)
                .ForContext(nameof(entry.FlowVersion), entry.FlowVersion)
                .ForContext(nameof(entry.Channel), entry.Channel)
                .ForContext(nameof(entry.Tag), entry.Tag)
                .ForContext(nameof(entry.TagSource), entry.TagSource)
                .ForContext(nameof(entry.Category), entry.Category.ToString())
                .ForContext(nameof(entry.Title), entry.Title)
                .ForContext(nameof(entry.IdMessage), entry.IdMessage)
                .ForContext(nameof(entry.From), entry.From)
                .ForContext(nameof(entry.OriginalFrom), entry.OriginalFrom)
                .ForContext(nameof(entry.To), entry.To)
                .ForContext(nameof(entry.OriginalTo), entry.OriginalTo)
                .ForContext(nameof(entry.Operation), entry.Operation)
                .ForContext(nameof(entry.EventType), entry.EventType)
                .ForContext(nameof(entry.Cluster), _cluster)
                .ForContext(nameof(entry.Data), entry.Data)
                .ForContext(nameof(entry.StateId), entry.StateId)
                .ForContext(nameof(entry.SensitiveData), entry.SensitiveData)
                .Write(level, entry.Title ?? UNTITLED_LOG);

            if (_checkIfMonitoringIsRegisteredFuncAsync == null)
            {
                SendLogToFireHoseAsync(entry);
                return;
            }

            if (_checkIfMonitoringIsRegisteredFuncAsync(entry.To).GetAwaiter().GetResult())
            {
                SendLogToFireHoseAsync(entry);
            }
        }

        public void SendLogToFireHoseAsync(LogEntry entry)
        {
            _fireHoseClient
                ?.SendLogToFireHoseAsync(entry, CancellationToken.None)
                .GetAwaiter()
                .GetResult();
        }

        private static LogEntry CreateLogEntry(
            LogCategory category,
            LogInput input,
            Exception? exception,
            string caller,
            string cluster = ""
        )
        {
            return new LogEntry
            {
                Category = category,
                Title = input.Title,
                IdMessage = input.IdMessage,
                From = input.From,
                OriginalFrom = input.OriginalFrom,
                To = input.To,
                OriginalTo = input.OriginalTo,
                Operation = input.Operation,
                EventType = input.EventType,
                Data = input.Data,
                Cluster = cluster,
                Exception = exception?.Message,
                TagSource = caller,
                FlowVersion = input.FlowVersion,
                Channel = input.Channel,
                SensitiveData = input.SensitiveData,
                StateId = input.StateId
            };
        }

        private static LogEventLevel ResolveLogLevel(
            LogCategory category,
            LogEventLevel? levelOverride
        )
        {
            return levelOverride
                ?? category switch
                {
                    LogCategory.ErrorEvents => LogEventLevel.Error,
                    _ => LogEventLevel.Information,
                };
        }

        /// <inheritdoc />
        public void MessageProcessing(LogInput input) =>
            LogMessage(LogCategory.MessageProcessing, input);

        /// <inheritdoc />
        public void ActionExecution(LogInput input) =>
            LogMessage(LogCategory.ActionExecution, input);

        /// <inheritdoc />
        public void UserContext(LogInput input) => LogMessage(LogCategory.UserContext, input);

        /// <inheritdoc />
        public void ConversationalFlow(LogInput input) =>
            LogMessage(LogCategory.ConversationalFlow, input);

        /// <inheritdoc />
        public void UserInput(LogInput input) => LogMessage(LogCategory.UserInput, input);

        /// <inheritdoc />
        public void MessageDelivery(LogInput input) =>
            LogMessage(LogCategory.MessageDelivery, input);

        /// <inheritdoc />
        public void MissingInfoLatency(LogInput input) =>
            LogMessage(LogCategory.MissingInfoLatency, input);

        /// <inheritdoc />
        public void ErrorEvents(LogInput input, Exception exception) =>
            LogMessage(LogCategory.ErrorEvents, input, exception);
    }
}
