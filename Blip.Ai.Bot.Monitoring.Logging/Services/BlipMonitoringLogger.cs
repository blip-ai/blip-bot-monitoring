using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Interface;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;
using Serilog.Sinks.Grafana.Loki;
using Serilog.Sinks.Grafana.Loki.HttpClients;
using System.Runtime.CompilerServices;

namespace BlipLogging.Services
{
    public class BlipMonitoringLogger : IBlipLogger
    {
        private readonly ILogger Logger;

        public BlipMonitoringLogger(LoggingOptions options)
        {
            var loggerConfig = new LoggerConfiguration()
                .Enrich.FromLogContext()
                .WriteTo.Console(new RenderedCompactJsonFormatter())
                .Enrich.WithProperty("service_name", "BlipMontoring");

            if (options.Serilog != null)
            {
                loggerConfig.WriteTo.Seq(
                    options.Serilog.Url,
                    apiKey: options.Serilog.ApiKey);
            }

            if (options.Grafana != null)
            {
                var credentials = !string.IsNullOrWhiteSpace(options.Grafana.LokiLogin) && !string.IsNullOrWhiteSpace(options.Grafana.LokiPassword)
                    ? new LokiCredentials
                    {
                        Login = options.Grafana.LokiLogin,
                        Password = options.Grafana.LokiPassword
                    }
                    : null;

#pragma warning disable CA2000
                var httpClient = new HttpClient();
                var lokiHttpClient = new LokiGzipHttpClient(httpClient);
#pragma warning restore CA2000

                if (!string.IsNullOrWhiteSpace(options.Grafana.LokiHeaderName) && !string.IsNullOrWhiteSpace(options.Grafana.LokiHeaderValue))
                {
                    httpClient.DefaultRequestHeaders.Add(options.Grafana.LokiHeaderName, options.Grafana.LokiHeaderValue);
                }

                loggerConfig.WriteTo.GrafanaLoki(
                    options.Grafana.LokiUri.ToString(),
                     propertiesAsLabels: new[] { "Category" },
                     textFormatter: new RenderedCompactJsonFormatter(),
                    credentials: credentials);
            }

            Serilog.Log.Logger = loggerConfig.CreateLogger();
            Logger = Serilog.Log.Logger;
        }

        private void Log(LogCategory category, LogInput input, string? ex = null, [CallerMemberName] string caller = "")
        {
            var entry = new Logging
            {
                Category = category,
                Title = input.Title,
                IdMessage = input.IdMessage,
                From = input.From,
                To = input.To,
                Operation = input.Operation,
                Data = input.Data,
                Ex = ex,
                TagSource = caller
            };

            var level = category == LogCategory.ErrorEvents ? LogEventLevel.Error : LogEventLevel.Information;

            Logger
            .ForContext("FlowId", entry.FlowId)
            .ForContext("Tag", entry.Tag)
            .ForContext("TagSource", entry.TagSource)
            .ForContext("Category", category.ToString())
            .ForContext("Title", entry.Title)
            .ForContext("IdMessage", entry.IdMessage)
            .ForContext("From", entry.From)
            .ForContext("To", entry.To)
            .ForContext("Operation", entry.Operation)
            .Write(level, entry.Title ?? "Log sem título");
        }

        public void MessageProcessing(LogInput input) => Log(LogCategory.MessageProcessing, input);
        public void ActionExecution(LogInput input) => Log(LogCategory.ActionExecution, input);
        public void UserContext(LogInput input) => Log(LogCategory.UserContext, input);
        public void ConversationalFlow(LogInput input) => Log(LogCategory.ConversationalFlow, input);
        public void UserInput(LogInput input) => Log(LogCategory.UserInput, input);
        public void MessageDelivery(LogInput input) => Log(LogCategory.MessageDelivery, input);
        public void MissingInfoLatency(LogInput input) => Log(LogCategory.MissingInfoLatency, input);
        public void ErrorEvents(LogInput input, Exception ex) => Log(LogCategory.ErrorEvents, input, ex.ToString());
    }
}
