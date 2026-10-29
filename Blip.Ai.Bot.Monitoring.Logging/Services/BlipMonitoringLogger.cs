using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Serilog;

namespace Blip.Ai.Bot.Monitoring.Logging.Services;

public class BlipMonitoringLogger : ILazyBlipLogger, IDisposable, IAsyncDisposable
{
    private readonly IKafkaLogClient? _kafkaLogClient;
    private readonly bool _ownsKafkaLogClient;
    private readonly bool _enabled;
    private readonly string _cluster;
    private readonly Func<string, Task<bool>>? _legacyCheck;
    private readonly Func<string, CancellationToken, Task<bool>>? _check;
    private readonly HashSet<string>? _filteredTitles;
    private readonly int _maximumPreparations;
    private readonly TimeSpan _eligibilityTimeout;
    private readonly TimeSpan _shutdownTimeout;
    private readonly object _gate = new();
    private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _pending;
    private bool _disposing;
    private Task? _shutdown;

    public MonitoringDiagnostics Diagnostics { get; } = new();
    public int ActivePreparations { get { lock (_gate) return _pending; } }

    public BlipMonitoringLogger(LoggingOptions options, bool ownsKafkaLogClient,
        Func<string, CancellationToken, Task<bool>>? eligibilityCheckAsync = null,
        IKafkaLogClient? kafkaLogClient = null, ILogger? logger = null,
        HashSet<string>? monitoringDetailedFilteredTitles = null)
        : this(options, null, kafkaLogClient, logger, monitoringDetailedFilteredTitles,
            ownsKafkaLogClient, eligibilityCheckAsync) { }

    // Keep the original constructor signature for already compiled consumers.
    public BlipMonitoringLogger(LoggingOptions options,
        Func<string, Task<bool>>? checkIfMonitoringIsRegisteredFuncAsync = null,
        IKafkaLogClient? kafkaLogClient = null, ILogger? logger = null,
        HashSet<string>? monitoringDetailedFilteredTitles = null)
        : this(options, checkIfMonitoringIsRegisteredFuncAsync, kafkaLogClient, logger,
            monitoringDetailedFilteredTitles, false, null) { }

    public BlipMonitoringLogger(LoggingOptions options,
        Func<string, Task<bool>>? checkIfMonitoringIsRegisteredFuncAsync,
        IKafkaLogClient? kafkaLogClient, ILogger? logger,
        HashSet<string>? monitoringDetailedFilteredTitles,
        bool ownsKafkaLogClient,
        Func<string, CancellationToken, Task<bool>>? eligibilityCheckAsync)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.MaxConcurrentPreparations <= 0 || options.EligibilityTimeoutMilliseconds <= 0 ||
            options.ShutdownTimeoutMilliseconds <= 0)
            throw new ArgumentException("Monitoring budgets must be positive.", nameof(options));
        _enabled = options.IsEnabledMonitoring;
        _cluster = options.Cluster ?? string.Empty;
        _legacyCheck = checkIfMonitoringIsRegisteredFuncAsync;
        _check = eligibilityCheckAsync;
        _maximumPreparations = options.MaxConcurrentPreparations;
        _eligibilityTimeout = TimeSpan.FromMilliseconds(options.EligibilityTimeoutMilliseconds);
        _shutdownTimeout = TimeSpan.FromMilliseconds(options.ShutdownTimeoutMilliseconds);
        _filteredTitles = monitoringDetailedFilteredTitles is { Count: > 0 }
            ? new HashSet<string>(monitoringDetailedFilteredTitles, StringComparer.OrdinalIgnoreCase) : null;
        _kafkaLogClient = kafkaLogClient;
        _ownsKafkaLogClient = ownsKafkaLogClient;
        if (_enabled && kafkaLogClient == null && options.Kafka?.IsValid() == true)
        {
            _kafkaLogClient = new KafkaLogClient(options.Kafka, logger, options.IsEnabledLoggingJsonParseErrors);
            _ownsKafkaLogClient = true;
        }
    }

    public void LogMessage(LogCategory category, LogInput input, Exception? exception = null)
    {
        ArgumentNullException.ThrowIfNull(input);
        TryLogMessage(category, input.To, () => input, exception);
    }

    public bool TryLogMessage(LogCategory category, string destination, Func<LogInput> inputFactory,
        Exception? exception = null)
    {
        ArgumentNullException.ThrowIfNull(inputFactory);
        lock (_gate)
        {
            if (_disposing) { Diagnostics.Record(MonitoringOutcome.Shutdown); return false; }
            if (!_enabled || _kafkaLogClient == null) { Diagnostics.Record(MonitoringOutcome.Disabled); return false; }
            if (_pending >= _maximumPreparations) { Diagnostics.Record(MonitoringOutcome.Full); return false; }
            _pending++;
            MonitoringDiagnostics.Preparation(1);
        }
        var transferred = false;
        var preparing = false;
        CancellationTokenSource? timeout = null;
        try
        {
            if (_check != null || _legacyCheck != null)
                timeout = new CancellationTokenSource(_eligibilityTimeout);
            var eligibility = _check != null ? _check(destination, timeout!.Token) :
                _legacyCheck != null ? _legacyCheck(destination) : Task.FromResult(true);
            if (eligibility == null)
            {
                Diagnostics.Record(MonitoringOutcome.Ineligible);
                return false;
            }
            if (!eligibility.IsCompleted)
            {
                Diagnostics.Record(MonitoringOutcome.Ineligible);
                // Never capture inputFactory/input/exception in asynchronous refresh work.
                transferred = true;
                _ = ObserveRefreshAsync(eligibility, timeout!);
                timeout = null;
                return false;
            }
            if (!eligibility.GetAwaiter().GetResult())
            {
                Diagnostics.Record(MonitoringOutcome.Ineligible);
                return false;
            }
            timeout?.Dispose();
            timeout = null;
            preparing = true;
            lock (_gate) { if (_disposing) { Diagnostics.Record(MonitoringOutcome.Shutdown); return false; } }
            if (_kafkaLogClient is ILazyKafkaLogClient lazyClient)
            {
                return lazyClient.TrySendLog(() =>
                {
                    var snapshot = inputFactory();
                    if (category != LogCategory.ErrorEvents && snapshot.Title != null &&
                        _filteredTitles?.Contains(snapshot.Title) == true)
                    {
                        Diagnostics.Record(MonitoringOutcome.Filtered);
                        return null;
                    }
                    lock (_gate)
                    {
                        if (_disposing) { Diagnostics.Record(MonitoringOutcome.Shutdown); return null; }
                    }
                    return KafkaLogPayload.FromInput(snapshot, category, _cluster, exception);
                });
            }
            var input = inputFactory();
            if (category != LogCategory.ErrorEvents && input.Title != null &&
                _filteredTitles?.Contains(input.Title) == true)
            {
                Diagnostics.Record(MonitoringOutcome.Filtered);
                return false;
            }
            lock (_gate) { if (_disposing) { Diagnostics.Record(MonitoringOutcome.Shutdown); return false; } }
            var send = SendLogToKafkaAsync(input, category, exception);
            if (!send.IsCompleted)
            {
                transferred = true;
                _ = ObserveSendAsync(send);
            }
            else send.GetAwaiter().GetResult();
            Diagnostics.Record(MonitoringOutcome.Admitted);
            return true;
        }
        catch (OperationCanceledException) when (!preparing && timeout?.IsCancellationRequested == true)
        {
            Diagnostics.Record(MonitoringOutcome.EligibilityTimeout);
            return false;
        }
        catch (Exception)
        {
            Diagnostics.Record(preparing ? MonitoringOutcome.SerializationError : MonitoringOutcome.Ineligible);
            return false;
        }
        finally
        {
            timeout?.Dispose();
            if (!transferred) CompletePreparation();
        }
    }

    private async Task ObserveRefreshAsync(Task<bool> task, CancellationTokenSource timeout)
    {
        try
        {
            try { await task.WaitAsync(_eligibilityTimeout).ConfigureAwait(false); }
            catch (TimeoutException)
            {
                Diagnostics.Record(MonitoringOutcome.EligibilityTimeout);
                // A legacy callback may ignore cancellation. Keep its slot until actual completion;
                // otherwise timed-out callbacks would accumulate without bound.
                await task.ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { Diagnostics.Record(MonitoringOutcome.EligibilityTimeout); }
        catch (Exception) { Diagnostics.Record(MonitoringOutcome.Ineligible); }
        finally { timeout.Dispose(); CompletePreparation(); }
    }

    private async Task ObserveSendAsync(Task task)
    {
        try { await task.ConfigureAwait(false); }
        catch (Exception) { Diagnostics.Record(MonitoringOutcome.DeliveryFailed); }
        finally { CompletePreparation(); }
    }

    private void CompletePreparation()
    {
        lock (_gate)
        {
            MonitoringDiagnostics.Preparation(-1);
            if (--_pending == 0 && _disposing) _drained.TrySetResult();
        }
    }

    internal Task SendLogToKafkaAsync(LogInput input, LogCategory category, Exception? exception = null) =>
        !_enabled || _kafkaLogClient == null ? Task.CompletedTask :
        _kafkaLogClient.SendLogAsync(KafkaLogPayload.FromInput(input, category, _cluster, exception),
            CancellationToken.None);

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            if (_shutdown != null) return new ValueTask(_shutdown);
            _disposing = true;
            if (_pending == 0) _drained.TrySetResult();
            _shutdown = ShutdownAsync();
            return new ValueTask(_shutdown);
        }
    }

    private async Task ShutdownAsync()
    {
        var cleanup = Task.CompletedTask;
        if (_ownsKafkaLogClient)
        {
            cleanup = Task.Run(async () =>
            {
                if (_kafkaLogClient is KafkaLogClient kafkaClient)
                {
                    // Closing the built-in client's admission is safe even if a snapshot factory
                    // is still returning. Preparation and Kafka draining share the same deadline.
                    await kafkaClient.DisposeAsync(_shutdownTimeout).ConfigureAwait(false);
                    return;
                }
                // Never dispose an arbitrary injected client concurrently with its active sends.
                await _drained.Task.ConfigureAwait(false);
                if (_kafkaLogClient is IAsyncDisposable asyncDisposable)
                    await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                else if (_kafkaLogClient is IDisposable disposable) disposable.Dispose();
            });
        }
        var shutdown = Task.WhenAll(_drained.Task, cleanup);
        try { await shutdown.WaitAsync(_shutdownTimeout).ConfigureAwait(false); }
        catch (TimeoutException) { Diagnostics.Record(MonitoringOutcome.Shutdown); }
        catch (Exception) { Diagnostics.Record(MonitoringOutcome.Shutdown); }
        _ = shutdown.ContinueWith(t => { _ = t.Exception; }, CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
        GC.SuppressFinalize(this);
    }

    public void MessageProcessing(LogInput input) => LogMessage(LogCategory.MessageProcessing, input);
    public void ActionExecution(LogInput input) => LogMessage(LogCategory.ActionExecution, input);
    public void UserContext(LogInput input) => LogMessage(LogCategory.UserContext, input);
    public void ConversationalFlow(LogInput input) => LogMessage(LogCategory.ConversationalFlow, input);
    public void UserInput(LogInput input) => LogMessage(LogCategory.UserInput, input);
    public void MessageDelivery(LogInput input) => LogMessage(LogCategory.MessageDelivery, input);
    public void MissingInfoLatency(LogInput input) => LogMessage(LogCategory.MissingInfoLatency, input);
    public void ErrorEvents(LogInput input, Exception exception) => LogMessage(LogCategory.ErrorEvents, input, exception);
}
