using System.Runtime.CompilerServices;
using Blip.Ai.Bot.Monitoring.Logging.Abstractions;
using Blip.Ai.Bot.Monitoring.Logging.Abstractions.Models;
using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Serilog;

namespace Blip.Ai.Bot.Monitoring.Logging.Services
{
    public class BlipMonitoringLogger : IBlipLogger, IDisposable, IAsyncDisposable
    {
        private readonly IKafkaLogClient? _kafkaLogClient;
        private readonly bool _isEnabledMonitoring = true;
        private readonly string _cluster = string.Empty;
        private readonly Func<string, Task<bool>>? _checkIfMonitoringIsRegisteredFuncAsync = null;
        private readonly ILogger? _logger;
        private int _pendingTasks;
        private volatile bool _disposing;
        private readonly TaskCompletionSource _drained = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

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
            _logger = logger;
        }

        /// <inheritdoc />
        public void LogMessage(LogCategory category, LogInput input, Exception? exception = null)
        {
            if (!_isEnabledMonitoring || _disposing)
            {
                return;
            }

            Interlocked.Increment(ref _pendingTasks);
            LogMessageAsync(input, category, exception)
                .ContinueWith(
                    OnTaskComplete,
                    null,
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default
                );
        }

        private async Task LogMessageAsync(
            LogInput input,
            LogCategory category,
            Exception? exception = null,
            [CallerMemberName] string callerName = ""
        )
        {
            try
            {
                if (!await ShouldSendToKafkaAsync(input.To).ConfigureAwait(false))
                {
                    return;
                }

                await SendLogToKafkaAsync(input, category, exception, callerName)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger?.Error(
                    ex,
                    "[{Source}] Failed to send log message to Kafka for user {from} and owner {Owner}",
                    nameof(BlipMonitoringLogger),
                    input.From,
                    input.To
                );
            }
        }

        private Task<bool> ShouldSendToKafkaAsync(string destination) =>
            _checkIfMonitoringIsRegisteredFuncAsync?.Invoke(destination) ?? Task.FromResult(true);

        private void OnTaskComplete(Task _, object? __)
        {
            if (Interlocked.Decrement(ref _pendingTasks) == 0 && _disposing)
                _drained.TrySetResult();
        }

        internal async Task SendLogToKafkaAsync(
            LogInput input,
            LogCategory category,
            Exception? exception = null,
            string tagSource = ""
        )
        {
            if (!_isEnabledMonitoring)
            {
                return;
            }

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

            await _kafkaLogClient
                .SendLogAsync(payload, CancellationToken.None)
                .ConfigureAwait(false);
        }

        public void Dispose()
        {
            _disposing = true;

            if (Interlocked.CompareExchange(ref _pendingTasks, 0, 0) > 0)
                _drained.Task.Wait(TimeSpan.FromSeconds(30));

            if (_kafkaLogClient is IDisposable disposable)
                disposable.Dispose();

            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync()
        {
            _disposing = true;
            if (Interlocked.CompareExchange(ref _pendingTasks, 0, 0) > 0)
                await _drained.Task.ConfigureAwait(false);

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
