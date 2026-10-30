using System.Diagnostics;
using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Blip.Ai.Bot.Monitoring.Logging.Services;

namespace Blip.Ai.Bot.Monitoring.Logging.LocalValidator;

internal static class SyntheticScenarios
{
    public static async Task RunAsync()
    {
        Console.WriteLine("Synthetic monitoring validation: no broker, no network, fixed bounded generator.");
        await HealthyAsync();
        await OverloadAsync();
        await OversizeAsync();
        await EligibilityAsync();
        await RecoveryAsync();
        await ShutdownAsync();
        Console.WriteLine("PASS: 6 synthetic scenarios. Native Kafka/RSS and real-broker contract NOT verified.");
    }

    private static KafkaOptions Options() => new()
    {
        BootstrapServers = "unused:9092", Topic = "synthetic-only",
        SaslUsername = "synthetic", SaslPassword = "synthetic",
        QueueCapacity = 4, QueueMaxBytes = 16 * 1024, MaxMessageBytes = 4096,
        BatchMaxCount = 4, BatchMaxBytes = 1, BatchMaxDelayMilliseconds = 10,
        MaxEventAgeMilliseconds = 10000, ShutdownTimeoutMilliseconds = 200
    };

    private static LogInput Input(object? data = null) => new()
    {
        FlowId = "synthetic-flow", Title = "synthetic", IdMessage = "synthetic-message",
        From = "synthetic-user", OriginalFrom = "synthetic-user", To = "synthetic-bot",
        OriginalTo = "synthetic-bot", Operation = "synthetic", EventType = "synthetic",
        StateId = "synthetic-state", Data = data
    };

    private static async Task HealthyAsync()
    {
        var publisher = new SyntheticPublisher(paused: false);
        await using var client = new KafkaLogClient(Options(), publisher);
        await using var logger = new BlipMonitoringLogger(new LoggingOptions(), null, client);
        for (var i = 0; i < 100; i++)
        {
            Check(logger.TryLogMessage(LogCategory.UserInput, "synthetic-bot", () => Input("small")), "healthy admission");
            await UntilAsync(() => client.RetainedCount == 0);
        }
        Check(publisher.Delivered == 100, "healthy delivery count");
        Console.WriteLine("PASS healthy: offered=100 delivered=100 retained=0");
    }

    private static async Task OverloadAsync()
    {
        var options = Options();
        var publisher = new SyntheticPublisher(paused: true);
        await using var client = new KafkaLogClient(options, publisher);
        await using var logger = new BlipMonitoringLogger(new LoggingOptions(), null, client);
        var snapshots = 0;
        for (var i = 0; i < 5000; i++)
        {
            logger.TryLogMessage(LogCategory.UserInput, "synthetic-bot", () =>
            {
                snapshots++;
                return Input("bounded");
            });
            Check(client.RetainedCount <= options.QueueCapacity, "count bound");
            Check(client.RetainedBytes <= options.QueueMaxBytes, "byte bound");
        }
        Check(snapshots == options.QueueCapacity, "no projections after saturation");
        Check(client.Diagnostics[MonitoringOutcome.Full] == 5000 - snapshots, "full accounting");
        publisher.Resume.TrySetResult();
        await client.DisposeAsync();
        Check(client.RetainedCount == 0 && client.RetainedBytes == 0, "overload recovery ownership");
        Console.WriteLine($"PASS overload: offered=5000 snapshots={snapshots} dropped={5000 - snapshots} retained=0");
    }

    private static async Task OversizeAsync()
    {
        await using var client = new KafkaLogClient(Options(), new SyntheticPublisher(paused: false));
        await using var logger = new BlipMonitoringLogger(new LoggingOptions(), null, client);
        Check(!logger.TryLogMessage(LogCategory.UserInput, "synthetic-bot",
            () => Input(new string('x', 1024 * 1024))), "oversize admission");
        Check(client.Diagnostics[MonitoringOutcome.Oversize] == 1, "oversize outcome");
        Check(client.RetainedBytes == 0 && client.RetainedCount == 0, "oversize ownership");
        Console.WriteLine("PASS oversize: offered=1 rejected=1 retained=0");
    }

    private static async Task EligibilityAsync()
    {
        var refresh = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var client = new KafkaLogClient(Options(), new SyntheticPublisher(paused: false));
        var logger = new BlipMonitoringLogger(new LoggingOptions
        {
            MaxConcurrentPreparations = 2, EligibilityTimeoutMilliseconds = 20,
            ShutdownTimeoutMilliseconds = 20
        }, _ => refresh.Task, client);
        var snapshots = 0;
        for (var i = 0; i < 5000; i++)
            logger.TryLogMessage(LogCategory.UserInput, "synthetic-bot", () =>
            {
                snapshots++;
                return Input();
            });
        await UntilAsync(() => logger.Diagnostics[MonitoringOutcome.EligibilityTimeout] == 2);
        Check(logger.ActivePreparations == 2 && snapshots == 0, "physical eligibility bound");
        Check(logger.Diagnostics[MonitoringOutcome.Full] == 4998, "eligibility overload accounting");
        await logger.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
        refresh.TrySetResult(true);
        await UntilAsync(() => logger.ActivePreparations == 0);
        Console.WriteLine("PASS eligibility: offered=5000 physical=2 snapshots=0 finite-shutdown=true");
    }

    private static async Task RecoveryAsync()
    {
        var options = Options();
        options.MaxEventAgeMilliseconds = 50;
        var publisher = new SyntheticPublisher(paused: true);
        await using var client = new KafkaLogClient(options, publisher);
        await client.SendLogAsync(KafkaLogPayload.FromInput(Input(), LogCategory.UserInput, "synthetic"));
        await publisher.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await client.SendLogAsync(KafkaLogPayload.FromInput(Input(), LogCategory.UserInput, "synthetic"));
        await Task.Delay(75);
        publisher.Resume.TrySetResult();
        await client.DisposeAsync();
        Check(client.Diagnostics[MonitoringOutcome.Expired] == 1, "expired backlog");
        Check(publisher.Delivered == 1 && client.RetainedBytes == 0, "recovery ownership");
        Console.WriteLine("PASS recovery: delivered=1 expired=1 retained=0");
    }

    private static async Task ShutdownAsync()
    {
        var options = Options();
        options.ShutdownTimeoutMilliseconds = 20;
        var publisher = new SyntheticPublisher(paused: true);
        var client = new KafkaLogClient(options, publisher);
        await client.SendLogAsync(KafkaLogPayload.FromInput(Input(), LogCategory.UserInput, "synthetic"));
        await publisher.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await client.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
        Check(client.RetainedCount == 1 && !publisher.Disposed.Task.IsCompleted, "late delivery ownership");
        Check(client.Diagnostics[MonitoringOutcome.Abandoned] == 1, "shutdown accounting");
        publisher.Resume.TrySetResult();
        await publisher.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Check(client.RetainedBytes == 0 && client.RetainedCount == 0, "late cleanup");
        Console.WriteLine("PASS shutdown: deadline=20ms abandoned=1 late-cleanup=true retained=0");
    }

    private static async Task UntilAsync(Func<bool> condition)
    {
        var started = Stopwatch.GetTimestamp();
        while (!condition() && Stopwatch.GetElapsedTime(started) < TimeSpan.FromSeconds(5))
            await Task.Delay(1);
        Check(condition(), "condition completed within five seconds");
    }

    private static void Check(bool success, string invariant)
    {
        if (!success) throw new InvalidOperationException($"Synthetic invariant failed: {invariant}");
    }

    private sealed class SyntheticPublisher : IKafkaLogBatchPublisher
    {
        public SyntheticPublisher(bool paused)
        {
            if (!paused) Resume.TrySetResult();
        }
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Resume { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Disposed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Delivered;
        public async Task PublishAsync(KafkaLogBatch batch, CancellationToken cancellationToken)
        {
            Entered.TrySetResult();
            await Resume.Task.ConfigureAwait(false);
            Interlocked.Add(ref Delivered, batch.Events.Length);
        }
        public void Dispose() => Disposed.TrySetResult();
    }
}
