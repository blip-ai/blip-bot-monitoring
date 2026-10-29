using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using Blip.Ai.Bot.Monitoring.Logging.Clients;
using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Blip.Ai.Bot.Monitoring.Logging.Serialization;
using Blip.Ai.Bot.Monitoring.Logging.Services;
using Confluent.Kafka;
using Moq;
using Newtonsoft.Json.Linq;

namespace Blip.Ai.Bot.Monitoring.Logging.Tests;

public class BoundedPipelineTests
{
    private static KafkaOptions Options() => new()
    {
        BootstrapServers = "unused:9092", Topic = "monitoring",
        SaslUsername = "test", SaslPassword = "test",
        QueueCapacity = 8, QueueMaxBytes = 32 * 1024, MaxMessageBytes = 4096,
        BatchMaxBytes = 1, BatchMaxDelayMilliseconds = 20, ShutdownTimeoutMilliseconds = 1000
    };

    private static LogInput Input(object? data = null) => new()
    {
        FlowId = "flow", Title = "test", IdMessage = "message", From = "user",
        OriginalFrom = "user", To = "bot", OriginalTo = "bot", Operation = "test",
        EventType = "test", Data = data, StateId = "state"
    };

    private static KafkaLogPayload Payload(object? data = null) =>
        KafkaLogPayload.FromInput(Input(data), LogCategory.UserInput, "test");

    private static async Task Eventually(Func<bool> condition)
    {
        var timer = Stopwatch.StartNew();
        while (!condition() && timer.Elapsed < TimeSpan.FromSeconds(5))
            await Task.Delay(5);
        Assert.True(condition());
    }

    [Fact]
    public async Task QueueCountIncludesInflightAndDropsImmediately()
    {
        var options = Options();
        options.QueueCapacity = 2;
        var publisher = new BlockingPublisher();
        await using var client = new KafkaLogClient(options, publisher);
        await client.SendLogAsync(Payload());
        await publisher.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await client.SendLogAsync(Payload());
        for (var i = 0; i < 1000; i++) Assert.True(client.SendLogAsync(Payload()).IsCompleted);
        Assert.Equal(2, client.RetainedCount);
        Assert.InRange(client.RetainedBytes, 1, options.QueueMaxBytes);
        Assert.Equal(1000, client.Diagnostics[MonitoringOutcome.Full]);
        publisher.Release.TrySetResult();
        await client.DisposeAsync();
        Assert.Equal(0, client.RetainedBytes);
        Assert.Equal(0, client.RetainedCount);
    }

    [Fact]
    public async Task ByteReservationIncludesInflightAndWorstCasePreparation()
    {
        var options = Options();
        options.QueueMaxBytes = options.MaxMessageBytes;
        var publisher = new BlockingPublisher();
        await using var client = new KafkaLogClient(options, publisher);
        await client.SendLogAsync(Payload());
        await publisher.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await client.SendLogAsync(Payload());
        Assert.Equal(1, client.RetainedCount);
        Assert.Equal(1, client.Diagnostics[MonitoringOutcome.Full]);
        publisher.Release.TrySetResult();
    }

    [Fact]
    public async Task ConcurrentSerializersAreNonwaitingAndBounded()
    {
        var options = Options();
        options.MaxConcurrentSerializations = 1;
        using var blocking = new BlockingValue();
        var publisher = new CapturingPublisher();
        await using var client = new KafkaLogClient(options, publisher);
        var send = Task.Run(() => client.SendLogAsync(Payload(blocking)));
        try
        {
            Assert.True(blocking.Entered.Wait(TimeSpan.FromSeconds(5)));
            Assert.Equal(options.MaxMessageBytes, client.RetainedBytes);
            Assert.True(client.SendLogAsync(Payload()).IsCompleted);
            Assert.Equal(1, client.Diagnostics[MonitoringOutcome.Full]);
        }
        finally { blocking.Release.Set(); }
        await send;
        await client.DisposeAsync();
        Assert.Equal(0, client.RetainedCount);
    }

    [Fact]
    public async Task HugeScalarIsRejectedBeforeLargeSerializerAllocation()
    {
        var options = Options();
        var payload = Payload(new string('x', 2 * 1024 * 1024));
        await using var client = new KafkaLogClient(options, new CapturingPublisher());
        var before = GC.GetAllocatedBytesForCurrentThread();
        await client.SendLogAsync(payload);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.InRange(allocated, 0, 512 * 1024);
        Assert.Equal(1, client.Diagnostics[MonitoringOutcome.Oversize]);
        Assert.Equal(0, client.RetainedBytes);
    }

    [Fact]
    public async Task DefaultMessageLimitPreservesPreviouslyAcceptedPayloadSize()
    {
        var options = Options();
        options.MaxMessageBytes = new KafkaOptions().MaxMessageBytes;
        options.QueueMaxBytes = 16 * 1024 * 1024;
        Assert.Equal(2097164, options.MaxMessageBytes);
        var publisher = new CapturingPublisher();
        await using var client = new KafkaLogClient(options, publisher);
        await client.SendLogAsync(Payload(new string('é', 200000)));
        await client.DisposeAsync();
        Assert.InRange(publisher.Batches.Single().Events.Single().Length, 256 * 1024, 2097164);
        Assert.Equal(0, client.Diagnostics[MonitoringOutcome.Oversize]);
    }

    [Fact]
    public async Task OversizedRawJsonIsRejectedBeforeMaterializingTokenTree()
    {
        var raw = new JRaw("{\"value\":\"" + new string('x', 1024 * 1024) + "\"}");
        await using var client = new KafkaLogClient(Options(), new CapturingPublisher());
        var before = GC.GetAllocatedBytesForCurrentThread();
        await client.SendLogAsync(Payload(raw));
        Assert.InRange(GC.GetAllocatedBytesForCurrentThread() - before, 0, 512 * 1024);
        Assert.Equal(1, client.Diagnostics[MonitoringOutcome.Oversize]);
        Assert.Equal(0, client.RetainedBytes);
    }

    [Fact]
    public void BoundedWriterAcceptsExactLimitAndRejectsExcess()
    {
        var buffer = new BoundedJsonBuffer(128);
        using (var writer = new Utf8JsonWriter(buffer)) writer.WriteStringValue(new string('x', 126));
        Assert.Equal(128, buffer.ToArray().Length);
        var tooLarge = new BoundedJsonBuffer(128);
        Assert.Throws<MessageTooLargeException>(() =>
        {
            using var writer = new Utf8JsonWriter(tooLarge);
            writer.WriteStringValue(new string('x', 127));
        });
    }

    [Fact]
    public async Task SerializationErrorReleasesBudgetAndWorkerSurvives()
    {
        var publisher = new CapturingPublisher();
        await using var client = new KafkaLogClient(Options(), publisher);
        await client.SendLogAsync(Payload((Action)(() => { })));
        Assert.Equal(1, client.Diagnostics[MonitoringOutcome.SerializationError]);
        Assert.Equal(0, client.RetainedCount);
        await client.SendLogAsync(Payload());
        await client.DisposeAsync();
        Assert.Single(publisher.Batches);
    }

    [Fact]
    public async Task CallerMutationAfterSendCannotChangeQueuedSnapshot()
    {
        var options = Options();
        options.BatchMaxBytes = 32000;
        options.BatchMaxDelayMilliseconds = 60000;
        var publisher = new CapturingPublisher();
        await using var client = new KafkaLogClient(options, publisher);
        var data = new JObject { ["text"] = "Início 🎉" };
        await client.SendLogAsync(Payload(data));
        data["text"] = "changed";
        await client.DisposeAsync();
        using var json = JsonDocument.Parse(publisher.Batches.Single().Events.Single());
        Assert.Equal("Início 🎉", json.RootElement.GetProperty("Data").GetProperty("text").GetString());
    }

    [Fact]
    public async Task AlreadyCanceledSendDoesNotReserveMemory()
    {
        await using var client = new KafkaLogClient(Options(), new CapturingPublisher());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            client.SendLogAsync(Payload(), new CancellationToken(true)));
        Assert.Equal(0, client.RetainedCount);
    }

    [Fact]
    public async Task CancellationDuringSerializationDropsWithoutQueueingOrLeaking()
    {
        using var blocking = new BlockingValue();
        using var cancellation = new CancellationTokenSource();
        var publisher = new CapturingPublisher();
        await using var client = new KafkaLogClient(Options(), publisher);
        var send = Task.Run(() => client.SendLogAsync(Payload(blocking), cancellation.Token));
        try
        {
            Assert.True(blocking.Entered.Wait(TimeSpan.FromSeconds(5)));
            cancellation.Cancel();
        }
        finally { blocking.Release.Set(); }
        await send;
        await client.DisposeAsync();
        Assert.Empty(publisher.Batches);
        Assert.Equal(1, client.Diagnostics[MonitoringOutcome.Canceled]);
        Assert.Equal(0, client.RetainedBytes);
    }

    [Fact]
    public async Task ExpiredQueuedEventsAreNotPublishedAfterRecovery()
    {
        var options = Options();
        options.MaxEventAgeMilliseconds = 100;
        var publisher = new BlockingPublisher();
        await using var client = new KafkaLogClient(options, publisher);
        await client.SendLogAsync(Payload());
        await publisher.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await client.SendLogAsync(Payload());
        await Task.Delay(150);
        publisher.Release.TrySetResult();
        await client.DisposeAsync();
        Assert.Equal(1, publisher.Calls);
        Assert.Equal(1, client.Diagnostics[MonitoringOutcome.Expired]);
        Assert.Equal(0, client.RetainedBytes);
    }

    [Fact]
    public async Task BatchCountIsBoundedIndependentOfByteBudget()
    {
        var options = Options();
        options.BatchMaxBytes = 32000;
        options.BatchMaxCount = 2;
        options.BatchMaxDelayMilliseconds = 60000;
        var publisher = new CapturingPublisher();
        await using var client = new KafkaLogClient(options, publisher);
        for (var i = 0; i < 3; i++) await client.SendLogAsync(Payload());
        await client.DisposeAsync();
        Assert.Equal(new[] { 2, 1 }, publisher.Batches.Select(batch => batch.Events.Length));
    }

    [Fact]
    public async Task PartialFailureDoesNotReplayAlreadyDeliveredRecords()
    {
        var options = Options();
        options.BatchMaxBytes = 32000;
        options.BatchMaxCount = 2;
        options.PublishRetryCount = 10;
        var publisher = new CapturingPublisher { Error = new PartialBatchException(1) };
        await using var client = new KafkaLogClient(options, publisher);
        await client.SendLogAsync(Payload());
        await client.SendLogAsync(Payload());
        await client.DisposeAsync();
        Assert.Single(publisher.Batches);
        Assert.Equal(1, client.Diagnostics[MonitoringOutcome.Delivered]);
        Assert.Equal(1, client.Diagnostics[MonitoringOutcome.DeliveryFailed]);
        Assert.Equal(0, client.RetainedCount);
    }

    [Fact]
    public async Task NoncooperativePublisherCannotExtendShutdownOrReleaseOwnershipEarly()
    {
        var options = Options();
        options.ShutdownTimeoutMilliseconds = 20;
        var publisher = new BlockingPublisher();
        var client = new KafkaLogClient(options, publisher);
        await client.SendLogAsync(Payload());
        await publisher.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await client.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
        Assert.False(publisher.Disposed.Task.IsCompleted);
        Assert.Equal(1, client.RetainedCount);
        Assert.Equal(1, client.Diagnostics[MonitoringOutcome.Abandoned]);
        publisher.Release.TrySetResult();
        await publisher.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, client.RetainedBytes);
    }

    [Fact]
    public async Task PublisherDisposalCannotExtendShutdownDeadline()
    {
        var options = Options();
        options.ShutdownTimeoutMilliseconds = 20;
        var publisher = new BlockingDisposePublisher();
        var client = new KafkaLogClient(options, publisher);
        try
        {
            await client.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
            Assert.True(publisher.Entered.Wait(TimeSpan.FromSeconds(5)));
            Assert.False(publisher.Finished.Task.IsCompleted);
        }
        finally { publisher.Release.Set(); }
        await publisher.Finished.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, publisher.Disposals);
        await client.DisposeAsync();
        publisher.Entered.Dispose();
        publisher.Release.Dispose();
    }

    [Fact]
    public async Task ShutdownDuringSerializationEventuallyReleasesExactlyOnce()
    {
        var options = Options();
        options.ShutdownTimeoutMilliseconds = 20;
        using var blocking = new BlockingValue();
        var publisher = new CapturingPublisher();
        var client = new KafkaLogClient(options, publisher);
        var send = Task.Run(() => client.SendLogAsync(Payload(blocking)));
        try
        {
            Assert.True(blocking.Entered.Wait(TimeSpan.FromSeconds(5)));
            await client.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
        }
        finally { blocking.Release.Set(); }
        await send;
        Assert.Empty(publisher.Batches);
        Assert.Equal(0, client.RetainedBytes);
        Assert.Equal(0, client.RetainedCount);
        Assert.Equal(1, client.Diagnostics[MonitoringOutcome.Shutdown]);
    }

    [Fact]
    public async Task MutatingOptionsCannotExpandEffectiveBudget()
    {
        var options = Options();
        options.QueueCapacity = 1;
        var publisher = new BlockingPublisher();
        await using var client = new KafkaLogClient(options, publisher);
        options.QueueCapacity = 100000;
        await client.SendLogAsync(Payload());
        await publisher.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await client.SendLogAsync(Payload());
        Assert.Equal(1, client.RetainedCount);
        Assert.Equal(1, client.Diagnostics[MonitoringOutcome.Full]);
        publisher.Release.TrySetResult();
    }

    [Fact]
    public async Task SaturatedClientRejectsBeforeAllocatingHostSnapshot()
    {
        var options = Options();
        options.QueueCapacity = 1;
        var publisher = new BlockingPublisher();
        await using var client = new KafkaLogClient(options, publisher);
        await using var logger = new BlipMonitoringLogger(new LoggingOptions(), null, client);
        var factories = 0;
        Assert.True(logger.TryLogMessage(LogCategory.UserInput, "bot", () => { factories++; return Input(); }));
        await publisher.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        for (var i = 0; i < 1000; i++)
            Assert.False(logger.TryLogMessage(LogCategory.UserInput, "bot", () => { factories++; return Input(); }));
        Assert.Equal(1, factories);
        Assert.Equal(1000, client.Diagnostics[MonitoringOutcome.Full]);
        publisher.Release.TrySetResult();
    }

    [Fact]
    public async Task AsyncEligibilityDropsWithoutRetainingOrInvokingFactory()
    {
        var refresh = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new Mock<IKafkaLogClient>();
        client.Setup(c => c.SendLogAsync(It.IsAny<KafkaLogPayload>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        await using var logger = new BlipMonitoringLogger(new LoggingOptions(), _ => refresh.Task, client.Object);
        var factories = 0;
        Assert.False(logger.TryLogMessage(LogCategory.UserInput, "bot", () => { factories++; return Input(); }));
        refresh.SetResult(true);
        await Eventually(() => logger.ActivePreparations == 0);
        Assert.Equal(0, factories);
        Assert.True(logger.TryLogMessage(LogCategory.UserInput, "bot", () => { factories++; return Input(); }));
        Assert.Equal(1, factories);
        client.Verify(c => c.SendLogAsync(It.IsAny<KafkaLogPayload>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task BrokenEligibilityCallbackCannotImplicitlyAllowMonitoring()
    {
        await using var logger = new BlipMonitoringLogger(new LoggingOptions(), _ => null!,
            Mock.Of<IKafkaLogClient>());
        Assert.False(logger.TryLogMessage(LogCategory.UserInput, "bot", () => throw new Exception("factory")));
        Assert.Equal(1, logger.Diagnostics[MonitoringOutcome.Ineligible]);
        Assert.Equal(0, logger.ActivePreparations);
    }

    [Fact]
    public async Task StalledEligibilityKeepsPhysicalSlotAfterTimeoutAndShutdownIsFinite()
    {
        var refresh = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = new LoggingOptions
        {
            MaxConcurrentPreparations = 2, EligibilityTimeoutMilliseconds = 20, ShutdownTimeoutMilliseconds = 20
        };
        var calls = 0;
        var logger = new BlipMonitoringLogger(options, _ => { calls++; return refresh.Task; },
            Mock.Of<IKafkaLogClient>());
        for (var i = 0; i < 1000; i++)
            Assert.False(logger.TryLogMessage(LogCategory.UserInput, "bot", () => throw new Exception("factory")));
        await Eventually(() => logger.Diagnostics[MonitoringOutcome.EligibilityTimeout] == 2);
        Assert.Equal(2, logger.ActivePreparations);
        Assert.Equal(2, calls);
        Assert.Equal(998, logger.Diagnostics[MonitoringOutcome.Full]);
        await logger.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
        refresh.SetResult(true);
        await Eventually(() => logger.ActivePreparations == 0);
    }

    [Fact]
    public async Task SnapshotFactoryIsNotRetainedOrSentAfterShutdownRace()
    {
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var client = new Mock<IKafkaLogClient>();
        var options = new LoggingOptions { ShutdownTimeoutMilliseconds = 20 };
        var logger = new BlipMonitoringLogger(options, null, client.Object);
        var send = Task.Run(() => logger.TryLogMessage(LogCategory.UserInput, "bot", () =>
        {
            entered.Set();
            if (!release.Wait(TimeSpan.FromSeconds(5))) throw new TimeoutException();
            return Input();
        }));
        try
        {
            Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
            await logger.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
        }
        finally { release.Set(); }
        Assert.False(await send);
        Assert.Equal(0, logger.ActivePreparations);
        client.Verify(c => c.SendLogAsync(It.IsAny<KafkaLogPayload>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task TokenAwareEligibilityReceivesFiniteCancellation()
    {
        CancellationToken observed = default;
        var options = new LoggingOptions { EligibilityTimeoutMilliseconds = 20 };
        await using var logger = new BlipMonitoringLogger(options, ownsKafkaLogClient: false,
            eligibilityCheckAsync: (_, token) =>
            {
                observed = token;
                return Task.Delay(Timeout.Infinite, token).ContinueWith(_ => false,
                    CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }, kafkaLogClient: Mock.Of<IKafkaLogClient>());
        Assert.False(logger.TryLogMessage(LogCategory.UserInput, "bot", () => throw new Exception("factory")));
        await Eventually(() => observed.IsCancellationRequested && logger.ActivePreparations == 0);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DisabledOrMissingClientNeverInvokesFactory(bool disabled)
    {
        var options = new LoggingOptions { IsEnabledMonitoring = !disabled };
        await using var logger = new BlipMonitoringLogger(options);
        Assert.False(logger.TryLogMessage(LogCategory.UserInput, "bot", () => throw new Exception("factory")));
        Assert.Equal(1, logger.Diagnostics[MonitoringOutcome.Disabled]);
        Assert.False(new NullBlipLogger().TryLogMessage(LogCategory.UserInput, "bot",
            () => throw new Exception("factory")));
    }

    [Fact]
    public async Task FactoryFailureReleasesPreparationAndDoesNotEscape()
    {
        await using var logger = new BlipMonitoringLogger(new LoggingOptions(), null, Mock.Of<IKafkaLogClient>());
        Assert.False(logger.TryLogMessage(LogCategory.UserInput, "bot", () => throw new InvalidOperationException()));
        Assert.Equal(0, logger.ActivePreparations);
        Assert.Equal(1, logger.Diagnostics[MonitoringOutcome.SerializationError]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ClientOwnershipIsExplicitAndDisposeIsIdempotent(bool owned)
    {
        var client = new DisposableClient();
        var logger = new BlipMonitoringLogger(new LoggingOptions(), owned, kafkaLogClient: client);
        await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => logger.DisposeAsync().AsTask()));
        Assert.Equal(owned ? 1 : 0, client.Disposals);
        Assert.False(logger.TryLogMessage(LogCategory.UserInput, "bot", () => throw new Exception("factory")));
    }

    [Fact]
    public async Task NoncooperativeInjectedSendKeepsPreparationBounded()
    {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new Mock<IKafkaLogClient>();
        client.Setup(c => c.SendLogAsync(It.IsAny<KafkaLogPayload>(), It.IsAny<CancellationToken>()))
            .Returns(completion.Task);
        var options = new LoggingOptions { MaxConcurrentPreparations = 1, ShutdownTimeoutMilliseconds = 20 };
        var logger = new BlipMonitoringLogger(options, null, client.Object);
        Assert.True(logger.TryLogMessage(LogCategory.UserInput, "bot", () => Input()));
        Assert.False(logger.TryLogMessage(LogCategory.UserInput, "bot", () => throw new Exception("factory")));
        await logger.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
        completion.SetResult();
        await Eventually(() => logger.ActivePreparations == 0);
    }

    [Fact]
    public async Task OwnedInjectedClientIsNotDisposedConcurrentlyWithActiveSend()
    {
        var client = new BlockingDisposableClient();
        var options = new LoggingOptions { ShutdownTimeoutMilliseconds = 20 };
        var logger = new BlipMonitoringLogger(options, ownsKafkaLogClient: true, kafkaLogClient: client);
        Assert.True(logger.TryLogMessage(LogCategory.UserInput, "bot", () => Input()));
        await logger.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(0, client.Disposals);
        client.Completion.SetResult();
        await Eventually(() => client.Disposals == 1);
        Assert.Equal(0, logger.ActivePreparations);
    }

    [Fact]
    public async Task OwnedBuiltInClientSharesLoggerShutdownDeadline()
    {
        var options = Options();
        options.ShutdownTimeoutMilliseconds = 5000;
        var publisher = new BlockingPublisher();
        var client = new KafkaLogClient(options, publisher);
        var logger = new BlipMonitoringLogger(new LoggingOptions { ShutdownTimeoutMilliseconds = 20 },
            ownsKafkaLogClient: true, kafkaLogClient: client);
        Assert.True(logger.TryLogMessage(LogCategory.UserInput, "bot", () => Input()));
        await publisher.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(logger.TryLogMessage(LogCategory.UserInput, "bot", () => Input()));
        await logger.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
        await Eventually(() => client.Diagnostics[MonitoringOutcome.Abandoned] > 0);
        publisher.Release.SetResult();
        await publisher.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, publisher.Calls);
        Assert.Equal(0, client.RetainedBytes);
        Assert.Equal(1, client.Diagnostics[MonitoringOutcome.Shutdown]);
    }

    [Fact]
    public async Task ByteProducerPreservesExactBytesNullKeyHeadersAndTopic()
    {
        var options = Options();
        var producer = new Mock<IProducer<Null, byte[]>>();
        var bytes = Encoding.UTF8.GetBytes("{\"text\":\"Início 🎉\"}");
        producer.Setup(p => p.ProduceAsync(options.Topic!, It.IsAny<Message<Null, byte[]>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, Message<Null, byte[]>, CancellationToken>((_, message, token) =>
            {
                Assert.Same(bytes, message.Value);
                Assert.Null(message.Key);
                Assert.Null(message.Headers);
                Assert.False(token.CanBeCanceled);
            }).ReturnsAsync(new DeliveryResult<Null, byte[]>());
        using var publisher = new KafkaLogBatchPublisher(options, producer.Object);
        await publisher.PublishAsync(new KafkaLogBatch { Events = new[] { bytes } }, CancellationToken.None);
        producer.Verify(p => p.ProduceAsync(options.Topic!, It.IsAny<Message<Null, byte[]>>(),
            It.IsAny<CancellationToken>()), Times.Once);
        Assert.Same(bytes, Serializers.ByteArray.Serialize(bytes,
            new SerializationContext(MessageComponentType.Value, options.Topic)));
    }

    [Fact]
    public async Task NativeQueueFullObservesSubmittedDeliveriesBeforeReportingPartialFailure()
    {
        var options = Options();
        var delivery = new TaskCompletionSource<DeliveryResult<Null, byte[]>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var producer = new Mock<IProducer<Null, byte[]>>();
        producer.SetupSequence(p => p.ProduceAsync(options.Topic!, It.IsAny<Message<Null, byte[]>>(),
                It.IsAny<CancellationToken>()))
            .Returns(delivery.Task).Throws(new KafkaException(new Error(ErrorCode.Local_QueueFull)));
        using var publisher = new KafkaLogBatchPublisher(options, producer.Object);
        var pending = publisher.PublishAsync(new KafkaLogBatch { Events = new[] { new byte[1], new byte[1] } },
            CancellationToken.None);
        Assert.False(pending.IsCompleted);
        delivery.SetResult(new DeliveryResult<Null, byte[]>());
        var error = await Assert.ThrowsAsync<PartialBatchException>(() => pending);
        Assert.Equal(1, error.Delivered);
        producer.Verify(p => p.ProduceAsync(options.Topic!, It.IsAny<Message<Null, byte[]>>(),
            It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task ManagedCancellationDoesNotReleaseNativeDeliveryOwnership()
    {
        var delivery = new TaskCompletionSource<DeliveryResult<Null, byte[]>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var producer = new Mock<IProducer<Null, byte[]>>();
        producer.Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<Null, byte[]>>(),
            It.IsAny<CancellationToken>())).Returns(delivery.Task);
        using var publisher = new KafkaLogBatchPublisher(Options(), producer.Object);
        using var cancellation = new CancellationTokenSource();
        var publish = publisher.PublishAsync(new KafkaLogBatch { Events = new[] { new byte[1] } },
            cancellation.Token);
        cancellation.Cancel();
        Assert.False(publish.IsCompleted);
        delivery.SetResult(new DeliveryResult<Null, byte[]>());
        await publish;
    }

    [Fact]
    public async Task NativePublicationRefusesUnboundedBatchFanout()
    {
        var options = Options();
        options.BatchMaxCount = 1;
        var producer = new Mock<IProducer<Null, byte[]>>();
        using var publisher = new KafkaLogBatchPublisher(options, producer.Object);
        await Assert.ThrowsAsync<ArgumentException>(() => publisher.PublishAsync(
            new KafkaLogBatch { Events = new[] { new byte[1], new byte[1] } }, CancellationToken.None));
        producer.Verify(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<Null, byte[]>>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public void ProducerHasExplicitNativeBudgetsAndDeliveryDeadline()
    {
        var options = Options();
        var config = KafkaLogBatchPublisher.CreateConfig(options);
        Assert.Equal(options.ProducerQueueMaxMessages, config.QueueBufferingMaxMessages);
        Assert.Equal(options.ProducerQueueMaxKilobytes, config.QueueBufferingMaxKbytes);
        Assert.Equal(options.DeliveryTimeoutMilliseconds, config.MessageTimeoutMs);
        Assert.Equal(options.DeliveryTimeoutMilliseconds, config.RequestTimeoutMs);
        Assert.Equal(Acks.Leader, config.Acks);
        Assert.False(config.EnableIdempotence);
        Assert.Equal(CompressionType.Zstd, config.CompressionType);
    }

    [Fact]
    public async Task DiagnosticsExportOnlyFiniteOutcomeLabels()
    {
        var tags = new ConcurrentBag<string>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == "Blip.Monitoring") meterListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, _, labels, _) =>
        {
            foreach (var label in labels) tags.Add($"{label.Key}:{label.Value}");
        });
        listener.Start();
        await using var logger = new BlipMonitoringLogger(new LoggingOptions());
        logger.TryLogMessage(LogCategory.UserInput, "customer-specific-secret-id", () => Input());
        Assert.Contains("outcome:Disabled", tags);
        Assert.All(tags, tag => Assert.Contains(tag,
            Enum.GetNames<MonitoringOutcome>().Select(name => $"outcome:{name}")));
    }

    [Fact]
    public void InconsistentBudgetsAreRejected()
    {
        var options = Options();
        options.QueueMaxBytes = options.MaxMessageBytes - 1;
        Assert.False(options.IsValid());
        Assert.Throws<ArgumentException>(() => new KafkaLogClient(options, new CapturingPublisher()));
        Assert.Throws<ArgumentException>(() => new BlipMonitoringLogger(
            new LoggingOptions { MaxConcurrentPreparations = 0 }));
    }

    private sealed class CapturingPublisher : IKafkaLogBatchPublisher
    {
        public ConcurrentQueue<KafkaLogBatch> Batches { get; } = new();
        public Exception? Error { get; init; }
        public Task PublishAsync(KafkaLogBatch batch, CancellationToken cancellationToken)
        {
            Batches.Enqueue(batch);
            return Error == null ? Task.CompletedTask : Task.FromException(Error);
        }
        public void Dispose() { }
    }

    private sealed class BlockingPublisher : IKafkaLogBatchPublisher
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Disposed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Calls;
        public async Task PublishAsync(KafkaLogBatch batch, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Calls);
            Entered.TrySetResult();
            await Release.Task.ConfigureAwait(false);
        }
        public void Dispose() => Disposed.TrySetResult();
    }

    private sealed class BlockingValue : IDisposable
    {
        [System.Text.Json.Serialization.JsonIgnore]
        public ManualResetEventSlim Entered { get; } = new();
        [System.Text.Json.Serialization.JsonIgnore]
        public ManualResetEventSlim Release { get; } = new();
        public string Value
        {
            get
            {
                Entered.Set();
                if (!Release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException();
                return "value";
            }
        }
        public void Dispose() { Entered.Dispose(); Release.Dispose(); }
    }

    private sealed class DisposableClient : IKafkaLogClient, IAsyncDisposable
    {
        public int Disposals;
        public Task SendLogAsync(KafkaLogPayload logEntry, CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() { Interlocked.Increment(ref Disposals); return ValueTask.CompletedTask; }
    }

    private sealed class BlockingDisposableClient : IKafkaLogClient, IAsyncDisposable
    {
        public TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Disposals;
        public Task SendLogAsync(KafkaLogPayload logEntry, CancellationToken cancellationToken) => Completion.Task;
        public ValueTask DisposeAsync() { Interlocked.Increment(ref Disposals); return ValueTask.CompletedTask; }
    }

    private sealed class BlockingDisposePublisher : IKafkaLogBatchPublisher
    {
        public ManualResetEventSlim Entered { get; } = new();
        public ManualResetEventSlim Release { get; } = new();
        public TaskCompletionSource Finished { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Disposals;
        public Task PublishAsync(KafkaLogBatch batch, CancellationToken cancellationToken) => Task.CompletedTask;
        public void Dispose()
        {
            Interlocked.Increment(ref Disposals);
            Entered.Set();
            Release.Wait(TimeSpan.FromSeconds(5));
            Finished.TrySetResult();
        }
    }
}
