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
    var options = new LoggingOptions
    {
        Serilog = new SerilogOptions
        {
            Url = "http://localhost:5342",
            ApiKey = "",
            ApplicationName = "MyApp"
        },
        Grafana = new GrafanaOptions()
        {
            LokiUri = "http://localhost:3100/",
            LokiLogin = "admin",
            LokiPassword = "12345",
        }
    };

    return new BlipMonitoringLogger(options);
});
```

### Kafka batching

Kafka delivery uses an in-memory bounded queue and publishes batches through Elephant. Events are flushed when the accumulated serialized payload reaches `BatchMaxBytes`, when `BatchMaxDelayMilliseconds` elapses, or when the logger is disposed during shutdown.

```csharp
var options = new LoggingOptions
{
  HostServiceName = "MyApp",
  Cluster = "prod",
  Kafka = new KafkaOptions
  {
    BootstrapServers = "kafka-1:9092,kafka-2:9092",
    Topic = "bot-monitoring-events",
    BatchMaxBytes = 1024 * 1024,
    BatchMaxDelayMilliseconds = 1000,
    QueueCapacity = 100000,
    ProducerLingerMilliseconds = 5,
    ProducerBatchSize = 128 * 1024,
    PublishRetryCount = 3,
    ShutdownTimeoutMilliseconds = 30000,
  },
};

await using var logger = new BlipMonitoringLogger(options);
```

Applications should dispose the logger on shutdown so the remaining in-memory events are drained and sent before the process exits. When the bounded queue reaches `QueueCapacity`, producers wait instead of allowing unbounded memory growth.

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