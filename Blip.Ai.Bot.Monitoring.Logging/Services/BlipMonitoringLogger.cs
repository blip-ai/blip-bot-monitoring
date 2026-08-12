using Blip.Ai.Bot.Monitoring.Logging.Abstractions;
using Blip.Ai.Bot.Monitoring.Logging.Abstractions.Models;
using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;

namespace Blip.Ai.Bot.Monitoring.Logging.Services
{
    public class BlipMonitoringLogger : IBlipLogger, IDisposable, IAsyncDisposable
    {
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
            IKafkaLogClient? kafkaLogClient = null
        )
        {
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

            _checkIfMonitoringIsRegisteredFuncAsync = checkIfMonitoringIsRegisteredFuncAsync;
        }

        /// <inheritdoc />
        public void LogMessage(LogCategory category, LogInput input, Exception? exception = null)
        {
            if (!_isEnabledMonitoring)
            {
                return;
            }

            _ = LogMessageAsync(input, category, exception);
        }

        private async Task LogMessageAsync(
            LogInput input,
            LogCategory category,
            Exception? exception = null
        )
        {
            try
            {
                if (!await ShouldSendToKafkaAsync(input.To).ConfigureAwait(false))
                {
                    return;
                }

                await SendLogToKafkaAsync(input, category, exception, category.ToString())
                    .ConfigureAwait(false);
            }
            catch
            {
            }
        }

        private Task<bool> ShouldSendToKafkaAsync(string destination) =>
            _checkIfMonitoringIsRegisteredFuncAsync?.Invoke(destination)
            ?? Task.FromResult(true);

        public async Task SendLogToKafkaAsync(
            LogInput input,
            LogCategory category,
            Exception? exception = null,
            string tagSource = ""
        )
        {
            var payload = KafkaLogPayload.FromInput(
                input,
                category,
                _cluster,
                exception,
                tagSource
            );

            if (_kafkaLogClient == null)
            {
                return;
            }

            await _kafkaLogClient.SendLogAsync(payload, CancellationToken.None)
                .ConfigureAwait(false);
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
