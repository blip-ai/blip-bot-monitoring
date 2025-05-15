using System.Runtime.CompilerServices;
using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;
using Serilog.Sinks.Grafana.Loki;
using Serilog.Sinks.Grafana.Loki.HttpClients;

namespace Blip.Ai.Bot.Monitoring.Logging.Services;

public class BlipMonitoringLogger : IBlipLogger
{
    private readonly ILogger _logger;

    public BlipMonitoringLogger(LoggingOptions options)
    {
        var loggerConfig = new LoggerConfiguration();

        if (options.Serilog != null)
        {
            loggerConfig.WriteTo.Seq(options.Serilog.Url, apiKey: options.Serilog.ApiKey);
        }

        if (options.Grafana != null)
        {
            var credentials =
                !string.IsNullOrWhiteSpace(options.Grafana.LokiLogin)
                && !string.IsNullOrWhiteSpace(options.Grafana.LokiPassword)
                    ? new LokiCredentials()
                    {
                        Login = options.Grafana.LokiLogin,
                        Password = options.Grafana.LokiPassword,
                    }
                    : null;

            // Loki Sink is responsible for disposing of the HttpClient. Disabling the warning with #pragma
#pragma warning disable CA2000
            var httpClient = new HttpClient();
            var lokiHttpClient = new LokiGzipHttpClient(httpClient);
#pragma warning restore CA2000

            if (
                !string.IsNullOrWhiteSpace(options.Grafana.LokiHeaderName)
                && !string.IsNullOrWhiteSpace(options.Grafana.LokiHeaderValue)
            )
            {
                httpClient.DefaultRequestHeaders.Add(
                    options.Grafana.LokiHeaderName,
                    options.Grafana.LokiHeaderValue
                );
            }

            var labels = new List<LokiLabel>();

            if (!string.IsNullOrWhiteSpace(options.Grafana.LokiLabels))
            {
                var labelsFromConfig = LabelsToDictionary(options.Grafana.LokiLabels);

                if (labelsFromConfig is not null)
                {
                    labels.AddRange(
                        labelsFromConfig.Select(x => new LokiLabel { Key = x.Key, Value = x.Value })
                    );
                }
            }

            loggerConfig.WriteTo.GrafanaLoki(
                options.Grafana.LokiUri,
                labels: labels,
                propertiesAsLabels: options.Grafana.LokiPropertiesAsLabels,
                restrictedToMinimumLevel: options.Grafana.LogLevel,
                textFormatter: new RenderedCompactJsonFormatter(),
                credentials: credentials
            );
        }

        _logger = loggerConfig.CreateLogger();
    }

    private void Log(
        LogCategory category,
        LogInput input,
        string? ex = null,
        [CallerMemberName] string caller = ""
    )
    {
        var entry = new Models.Logging
        {
            Category = category,
            Title = input.Title,
            IdMessage = input.IdMessage,
            From = input.From,
            To = input.To,
            Operation = input.Operation,
            Data = input.Data,
            Ex = ex,
            TagSource = caller,
        };

        var level =
            category == LogCategory.ErrorEvents ? LogEventLevel.Error : LogEventLevel.Information;
        _logger.Write(level, "{@Log}", entry);
    }

    public void MessageProcessing(LogInput input) => Log(LogCategory.MessageProcessing, input);

    public void ActionExecution(LogInput input) => Log(LogCategory.ActionExecution, input);

    public void UserContext(LogInput input) => Log(LogCategory.UserContext, input);

    public void ConversationalFlow(LogInput input) => Log(LogCategory.ConversationalFlow, input);

    public void UserInput(LogInput input) => Log(LogCategory.UserInput, input);

    public void MessageDelivery(LogInput input) => Log(LogCategory.MessageDelivery, input);

    public void MissingInfoLatency(LogInput input) => Log(LogCategory.MissingInfoLatency, input);

    public void ErrorEvents(LogInput input, Exception ex) =>
        Log(LogCategory.ErrorEvents, input, ex.ToString());

    private static Dictionary<string, string>? LabelsToDictionary(string values)
    {
        if (string.IsNullOrWhiteSpace(values))
        {
            return null;
        }

        return values
            .Split(';')
            .Select(x => x.Split('='))
            .Where(x => x.Length == 2)
            .ToDictionary(x => x[0], x => x[1]);
    }
}
