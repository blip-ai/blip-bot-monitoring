# Blip Monitoring

## What is Blip Monitoring?
Blip Monitoring is a core product of the Blip platform, designed to provide end-to-end monitoring of user journeys during interactions with intelligent contacts. It offers visibility, analysis, and control over bot behavior and conversational flows, evolving from the widely used Beholder extension.

## Log Structure

### Log Levels by Category
| Log Level | Associated Categories         | Notes                                      |
|-----------|--------------------------------|--------------------------------------------|
| Debug     | Message Processing, Action Execution, User Context, Flow | Ideal for development environments.       |
| Info      | User Input, Message Delivery  | Indicates normal operation.                |
| Warning   | Missing Information, High Latency | Abnormal situations without interruptions. |
| Error     | Error Events                  | Failures requiring attention.              |

### Log Categories
| Category            | Description                              | Examples                                   |
|---------------------|------------------------------------------|-------------------------------------------|
| Message Processing  | Captures bot interpretation of input     | Detected intent, fallback used            |
| Action Execution    | Records bot actions                     | API calls, script execution               |
| User Context        | Logs user journey variables              | User ID, custom attributes                |
| Conversational Flow | Tracks user movement within the flow     | Block entered, state transitions          |
| User Input          | Logs user messages                      | Typed text, button clicks                 |
| Message Delivery    | Indicates delivery and read status       | Delivered, read, delivery errors          |
| Missing Info/Latency| Captures missing data or high latency    | Not found data, slow response             |
| Error Events        | Captures critical failures              | API timeout, script error                 |

### Logged Fields Structure
| Field       | Description                          | Required |
|-------------|--------------------------------------|----------|
| FlowId      | Unique ID for the flow instance      | Yes      |
| Tag         | Name of the log package             | Yes      |
| TagSource   | Class or method triggering the log   | Yes      |
| Category    | Log category                        | Yes      |
| Operation   | Action or operation name            | Optional |
| Title       | Summary title of the event          | Yes      |
| IdMessage   | ID of the related message           | Yes      |
| datetime    | Date and time of the log            | Yes      |
| From        | Interaction origin (Bot/User)       | Yes      |
| To          | Interaction destination (Bot/User)  | Yes      |
| Data        | Additional metadata                 | Optional |
| Ex          | Exception stack trace (if any)      | Conditional |

## Logging Examples

### Example 1: Dependency Injection for Blip Logger

```csharp
container.RegisterSingleton<IBlipLogger>(() =>
{
    var options = configuration.GetSection("Monitoring").Get<LoggingOptions>();

    return new BlipMonitoringLogger(options);
});
```

### Bounded, best-effort Kafka delivery

Only monitoring events are best effort; never use this pipeline for customer messages or business state.
The existing `IBlipLogger` API is unchanged. Logging no longer waits for queue space or retries entire
batches. Full, expired, oversized, invalid, unregistered, or shutting-down events are dropped.
Asynchronous/unknown eligibility drops the **current** event while observing the bounded refresh.
The host should supply a bounded single-flight eligibility cache; subsequent cache hits must return
an already-completed task. Exceptions/negative results are not permission to log.
The library's 1000 ms eligibility timeout may be shorter than a host's physical lookup timeout
(for example, 5000 ms). In that case the current event is still dropped and the physical slot stays
charged until the lookup actually finishes; a later cache hit can log normally.

The channel owns only immutable UTF-8 bytes, not `LogInput`, `JObject`, exceptions, or host context graphs.
The item and byte budgets include preparation reservations, queued records, the active batch, and
managed ownership until actual delivery reports. A full-size byte reservation is acquired before
snapshot creation/serialization, then reduced to exact serialized length. This conservative policy
can drop an event even when its eventual small size would fit.

The following are **library safety defaults**, not measured production sizing recommendations:

```csharp
var options = new LoggingOptions
{
  HostServiceName = "MyApp",
  Cluster = "prod",
  MaxConcurrentPreparations = 4,
  EligibilityTimeoutMilliseconds = 1000,
  ShutdownTimeoutMilliseconds = 5000,
  Kafka = new KafkaOptions
  {
    BootstrapServers = "kafka-1:9092,kafka-2:9092",
    Topic = "bot-monitoring-events",
    SaslUsername = "<from secure configuration>",
    SaslPassword = "<from secure configuration>",
    BatchMaxBytes = 1024 * 1024,
    BatchMaxCount = 128,
    BatchMaxDelayMilliseconds = 1000,
    QueueCapacity = 1024,
    QueueMaxBytes = 16 * 1024 * 1024,
    MaxMessageBytes = 2097164,
    MaxConcurrentSerializations = 2,
    MaxEventAgeMilliseconds = 10000,
    ProducerLingerMilliseconds = 5,
    ProducerBatchSize = 128 * 1024,
    ProducerQueueMaxMessages = 1024,
    ProducerQueueMaxKilobytes = 16 * 1024,
    DeliveryTimeoutMilliseconds = 5000,
    ShutdownTimeoutMilliseconds = 5000,
  },
};

await using var logger = new BlipMonitoringLogger(options);
```

`MaxMessageBytes` preserves the previous 2097164-byte event contract.
`QueueCapacity` now defaults to 1024 (previously 100000). Configure these explicitly after measuring payload
quantiles, broker limits, RSS headroom, and offered byte rate. All budgets must be positive;
`QueueMaxBytes >= MaxMessageBytes`, and delivery timeout must exceed producer linger.
The obsolete-in-behavior `PublishRetryCount` and `WriteTimeoutMilliseconds` properties remain for
source/binary compatibility but do not enable replay or waiting. Native Kafka retries, if any, are
bounded by the delivery deadline. Idempotence remains disabled; acks remain leader-only.

Serialization uses a bounded writer, rejecting growth **before** allocating an oversized scalar's
requested buffer. The output cap is `MaxMessageBytes`; UTF-8 escaping may require up to
`6 * MaxMessageBytes + 4096` bytes of writer workspace. Geometric growth can briefly retain the
previous workspace (at most another workspace), plus one exact-sized output copy per active
serializer. Account for these separately from `QueueMaxBytes`, with at most
`MaxConcurrentSerializations` workspaces. Raw JSON parsing is additionally limited to
`MaxMessageBytes` characters and uses two forward-only normalization passes, not a second token tree.
Account separately for the bounded Newtonsoft reader's character/string workspace. Arbitrary
caller factories/getters can allocate or block independently: they must be short, synchronous, and
bounded. Unsupported object types are dropped, not stringified into potentially enormous graphs.
Standard Newtonsoft token conversions and permissive raw-JSON normalization remain supported.

Each event is still one JSON Kafka record with the configured topic, null key, and no headers
(the previous logger supplied no header provider). Confluent's byte-array serializer avoids the old
UTF-8/string/UTF-8 round trip; compression remains Zstd. Publication fan-out is bounded by
`BatchMaxCount`. Partial successes are counted and **never replayed as a whole batch**. A record
larger than `BatchMaxBytes` but within `MaxMessageBytes` is sent alone. Stale queued/batched events
are discarded before publication; an in-flight event can live for its age budget plus delivery timeout.
Native queue limits bound payload buffering, not total native/compression/socket overhead or RSS.

### Allocate host snapshots only after admission

```csharp
// eligibilityCheckAsync should return synchronously on cache hits; it must bound its own cache
// and physical refreshes. The old Func<string, Task<bool>> constructor is still supported.
await using var logger = new BlipMonitoringLogger(
    options,
    ownsKafkaLogClient: false,
    eligibilityCheckAsync: (destination, cancellationToken) =>
        registrationCache.CheckAsync(destination, cancellationToken));

// Use the SAME destination that the old LogInput.To contained.
logger.TryLogMessage(LogCategory.UserInput, destination, () => BuildIsolatedLogInput());
```

`ILazyBlipLogger.TryLogMessage(category, destination, factory, exception)` never retains its factory:
it runs synchronously after front admission, a positive immediate eligibility result, and downstream
count/byte/serialization reservation, or not at all. The factory must return an isolated snapshot;
do not mutate it concurrently while serializing. Subsequent caller mutations cannot change queued
bytes. `true` means an event was admitted, **not delivered**. Existing eager methods cannot avoid
allocations that callers performed before invoking the logger. Custom `IKafkaLogClient`
implementations should also implement `ILazyKafkaLogClient` for downstream pre-snapshot admission;
otherwise only the logger's preparation bound applies.

One host owner should dispose a shared logger, never individual bot scopes. Internally created clients
are owned; injected clients are **borrowed by default**. Pass `ownsKafkaLogClient: true` only when
transferring ownership. Concurrent/repeated disposal shares one shutdown operation. Admission stops
first; draining and cleanup share a finite total logger deadline. A non-cooperative eligibility callback
or injected send keeps its physical preparation slot until actual completion, even after timeout.
Shutdown returns without waiting forever. Pending native/injected deliveries keep their byte/count
charges until completion, then clean up exactly once; their buffers are never pooled or reused early.
Unfinished work at shutdown is reported as abandoned, not guaranteed delivered.

### Counters and local validation

The built-in `System.Diagnostics.Metrics` meter `Blip.Monitoring` exports `monitoring.events` with
only the finite `outcome` label (no issuer, bot, message, or exception labels), and aggregate
`monitoring.retained.bytes` / `monitoring.retained.count` / `monitoring.preparations.active` up/down counters.
`KafkaLogClient.Diagnostics`, `RetainedBytes`, `RetainedCount`, and
`BlipMonitoringLogger.Diagnostics` / `ActivePreparations` expose per-instance atomic snapshots.
Outcomes include admitted, delivered, full, disabled, filtered, ineligible, eligibility timeout,
oversize, serialization error, expired, delivery failed, canceled, shutdown, and abandoned.
Admitted/abandoned and eventual delivery outcomes can overlap; eligibility timeouts also overlap
the current event's ineligible drop. Do not sum these operational counters as disjoint events.
Alert on sustained drops, abandonment, and saturated ownership, not individual event exceptions.

Run the broker-free deterministic scenarios with:

```powershell
dotnet run --project .\Blip.Ai.Bot.Monitoring.Logging.LocalValidator -- --synthetic
dotnet test .\Blip.Ai.Bot.Monitoring.Logging.Tests
```

The synthetic generator is sequential and fixed-size; scenarios include healthy delivery, overload,
large payload rejection, stalled eligibility, recovery, and finite shutdown with accounting assertions.
These tests do **not** verify real Kafka interoperability, native memory, compression overhead,
production RSS, or throughput. A controlled isolated-broker contract/load run and coordinated package
release remain required before production rollout; no production budget should be inferred here.

### Example 2: C# Style Logging

```csharp
Log.ActionExecution(
    title: "Message sent via API",
    idMessage: context.Id,
    operation: "CallAPI",
    data: new 
    { 
        statusCode = 200, 
        endpoint = "/clients" 
    }
);
```

### Example 3: Structured JSON Log (for tools like Seq)

```json
{
  "@t": "2025-04-15T14:12:30.1234567Z",
  "@mt": "ActionExecution: {Title}",
  "@l": "Debug",
  "FlowId": "9b2d3c4e-a7f6-48b9-bb72-0870e3ef67c3",
  "Tag": "BlipMonitoring",
  "TagSource": "SendMessageService.Send",
  "Operation": "CallAPI",
  "Title": "Message sent via API",
  "IdMessage": "ad12cf34-7890-4567-bcde-ff2200aa1133",
  "From": "Bot",
  "To": "User",
  "Data": {
    "statusCode": 200,
    "endpoint": "/clients"
  },
  "Ex": null
}
```

**Notes:**  
- `@t` is the timestamp, auto-generated by Serilog.  
- `@mt` is the message template, customizable or inferred from method usage.  
- `@l` is the log level (e.g., Debug, Info).  
- Fields like FlowId, Tag, TagSource, From, To, and datetime are auto-populated by the library and do not require manual input.  

### Sending Logs to Grafana Cloud via HTTP
To push logs to Grafana Loki, you'll use the `/loki/api/v1/push` endpoint. Below is a step-by-step guide on how to authenticate and send logs.

#### Authentication
Grafana Cloud Loki requires Basic Auth with:
- Username: your instance ID (e.g., 1187314)
- Password: your Grafana API key with write access to Loki

#### Example Request

**Endpoint:**  
```
https://logs-prod-030.grafana.net/loki/api/v1/push
```

**Headers:**  
```
Authorization: Basic <base64(username:password)>
Content-Type: application/json
```

**Payload (JSON):**  
```json
{
  "streams": [
    {
      "stream": {
        "ambiente": "producao",
        "aplicacao": "blip-monitoring",
        "level": "Debug",
        "service_name": "SendMessageService",
        "FlowId": "9b2d3c4e-a7f6-48b9-bb72-0870e3ef67c3",
        "Tag": "BlipMonitoring",
        "TagSource": "SendMessageService.Send",
        "Category": "ActionExecution",
        "Operation": "CallAPI",
        "Title": "Message sent via API",
        "IdMessage": "ad12cf34-7890-4567-bcde-ff2200aa1133",
        "From": "Bot",
        "To": "User",
        "datetime": "2025-04-15T14:12:30.1234567Z"
      },
      "values": [
        [
          "1744829716523000000",
          "{"Data":{"statusCode":200,"endpoint":"/clients"},"Ex":null}"
        ]
      ]
    }
  ]
}
```

**Using cURL:**  
```bash
curl --location 'https://logs-prod-024.grafana.net/loki/api/v1/push' --header 'Content-Type: application/json' --header 'Authorization: Basic <TOKEN>' --data '{
  "streams": [
    {
      "stream": {
        "ambiente": "producao",
        "aplicacao": "blip-monitoring",
        "level": "Debug",
        "service_name": "SendMessageService",
        "FlowId": "9b2d3c4e-a7f6-48b9-bb72-0870e3ef67c3",
        "Tag": "BlipMonitoring",
        "TagSource": "SendMessageService.Send",
        "Category": "ActionExecution",
        "Operation": "CallAPI",
        "Title": "Message sent via API",
        "IdMessage": "ad12cf34-7890-4567-bcde-ff2200aa1133",
        "From": "Bot",
        "To": "User",
        "datetime": "2025-04-15T14:12:30.1234567Z"
      },
      "values": [
        [
          "1744892258890000000",
          "{"Data":{"statusCode":200,"endpoint":"/clients"},"Ex":null}"
        ]
      ]
    }
  ]
}'
```

---

For more details, visit the [Wiki Blip Monitoring](https://curupira.visualstudio.com/Takepedia/_wiki/wikis/Takepedia.wiki/31541/Blip-Monitoring).