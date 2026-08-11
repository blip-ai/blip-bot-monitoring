using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;
using Blip.Ai.Bot.Monitoring.Logging.Abstractions;
using Blip.Ai.Bot.Monitoring.Logging.Abstractions.Models;
using LogEntry = Blip.Ai.Bot.Monitoring.Logging.Models.Logging;

namespace Blip.Ai.Bot.Monitoring.Logging.Services
{
    public class BlipMonitoringLogger : IBlipLogger, IDisposable, IAsyncDisposable
    {
        private const int DEFAULT_BATCH_POSTING_LIMIT = 1000;
        private const string UNTITLED_LOG = "Untitled log";
        private const string HOST_SERVICE_NAME = "HostServiceName";
        private readonly ILogger Logger;
        private readonly IKafkaLogClient? _kafkaLogClient;
        private readonly bool _isEnabledMonitoring = true;
        private readonly string _cluster = string.Empty;
        private readonly Func<string, Task<bool>>? _checkIfMonitoringIsRegisteredFuncAsync = null;

        /// <summary>
        /// Initializes a new instance of the <see cref="BlipMonitoringLogger"/> class with the specified options.
        /// </summary>
        /// <param name="options">The logging options for configuring Serilog sinks.</param>
        /// <param name="checkIfMonitoringIsRegisteredFuncAsync">An optional function to determine if monitoring is enabled for a specific destination.</param>
        /// <param name="kafkaLogClient">An optional Kafka log client for sending logs. If not provided, a new instance will be created if Kafka options are valid.</param>
        public BlipMonitoringLogger(
            LoggingOptions options,
            Func<string, Task<bool>>? checkIfMonitoringIsRegisteredFuncAsync = null,
            IKafkaLogClient? kafkaLogClient = null,
            ILogger? logger = null
        )
        {
            var loggerConfig = CreateBaseLoggerConfiguration(options);
            ConfigureSeqSink(loggerConfig, options.Serilog);
            ConfigureConsoleErrorSink(loggerConfig);
            _isEnabledMonitoring = options.IsEnabledMonitoring;
            _cluster = options.Cluster ?? string.Empty;

            if (kafkaLogClient != null)
            {
                _kafkaLogClient = kafkaLogClient;
            }
            else if (options.Kafka != null && options.Kafka.IsValid())
            {
                _kafkaLogClient = new KafkaLogClient(options.Kafka);
            }

            Log.Logger = loggerConfig.CreateLogger();
            Logger = logger ?? Log.Logger;

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
            LogEventLevel? levelOverride = null
        )
        {
            if (!_isEnabledMonitoring)
            {
                return;
            }

            var level = ResolveLogLevel(category, levelOverride);

            
            EnrichLogger(Logger, input, category, category.ToString(), _cluster)
                .Write(level, input.Title ?? UNTITLED_LOG);

            if (ShouldSendToKafka(input.To))
            {
                SendLogToKafkaAsync(input, category, exception, category.ToString());
            }
        }

        private static ILogger EnrichLogger(
            ILogger logger,
            LogInput input,
            LogCategory category,
            string tagSource,
            string cluster
        ) =>
            logger
                .ForContext(nameof(LogInput.FlowId), input.FlowId)
                .ForContext(nameof(LogInput.FlowVersion), input.FlowVersion)
                .ForContext(nameof(LogInput.Channel), input.Channel)
                .ForContext(nameof(LogInput.Title), input.Title)
                .ForContext(nameof(LogInput.IdMessage), input.IdMessage)
                .ForContext(nameof(LogInput.From), input.From)
                .ForContext(nameof(LogInput.OriginalFrom), input.OriginalFrom)
                .ForContext(nameof(LogInput.To), input.To)
                .ForContext(nameof(LogInput.OriginalTo), input.OriginalTo)
                .ForContext(nameof(LogInput.Operation), input.Operation)
                .ForContext(nameof(LogInput.EventType), input.EventType)
                .ForContext(nameof(LogInput.Data), input.Data)
                .ForContext(nameof(LogInput.StateId), input.StateId)
                .ForContext(nameof(LogInput.SensitiveData), input.SensitiveData)
                .ForContext("Cluster", cluster)
                .ForContext("Tag", "BlipMonitoring")
                .ForContext("TagSource", tagSource)
                .ForContext("Category", category.ToString());

        private bool ShouldSendToKafka(string destination) =>
            _checkIfMonitoringIsRegisteredFuncAsync == null
            || _checkIfMonitoringIsRegisteredFuncAsync(destination).GetAwaiter().GetResult();

        public void SendLogToKafkaAsync(
            LogInput input,
            LogCategory category,
            Exception? exception = null,
            string tagSource = ""
        )
        {
            var entry = new LogEntry
            {
                FlowId = input.FlowId,
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
                Cluster = _cluster,
                Exception = exception?.Message,
                TagSource = tagSource,
                FlowVersion = input.FlowVersion,
                Channel = input.Channel,
                SensitiveData = input.SensitiveData,
                StateId = input.StateId,
            };

            _kafkaLogClient
                ?.SendLogAsync(entry, CancellationToken.None)
                .GetAwaiter()
                .GetResult();
        }

        public void Dispose()
        {
            if (_kafkaLogClient is IDisposable disposable)
            {
                disposable.Dispose();
            }

            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync()
        {
            if (_kafkaLogClient is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
            }
            else if (_kafkaLogClient is IDisposable disposable)
            {
                disposable.Dispose();
            }

            GC.SuppressFinalize(this);
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
