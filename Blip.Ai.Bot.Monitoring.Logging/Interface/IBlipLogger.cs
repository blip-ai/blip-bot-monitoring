using Blip.Ai.Bot.Monitoring.Logging.Enums;
using Blip.Ai.Bot.Monitoring.Logging.Models;
using Serilog.Events;
using System.Runtime.CompilerServices;

namespace Blip.Ai.Bot.Monitoring.Logging.Interface;

/// <summary>
/// Interface for logging monitoring events and data in the Blip bot environment.
/// </summary>
public interface IBlipLogger
{
    /// <summary>
    /// Logs a general message with optional exception and log level override.
    /// </summary>
    /// <param name="category">The log category to classify the message.</param>
    /// <param name="input">The log input data containing message and context.</param>
    /// <param name="exception">The related exception, if any.</param>
    /// <param name="levelOverride">The log level to override the default level, if provided.</param>
    /// <param name="caller">The name of the calling method.</param>
    void LogMessage(
      LogCategory category,
      LogInput input,
      Exception? exception = null,
      LogEventLevel? levelOverride = null,
      [CallerMemberName] string caller = "");

    /// <summary>
    /// Captures how the bot interprets and handles user input.
    /// </summary>
    /// <param name="input">The log input data.</param>
    void MessageProcessing(LogInput input);

    /// <summary>
    /// Records actions taken by the bot.
    /// </summary>
    /// <param name="input">The log input data.</param>
    void ActionExecution(LogInput input);

    /// <summary>
    /// Records user journey variables and data.
    /// </summary>
    /// <param name="input">The log input data.</param>
    void UserContext(LogInput input);

    /// <summary>
    /// Tracks user movement within the flow.
    /// </summary>
    /// <param name="input">The log input data.</param>
    void ConversationalFlow(LogInput input);

    /// <summary>
    /// Logs messages sent by the user.
    /// </summary>
    /// <param name="input">The log input data.</param>
    void UserInput(LogInput input);

    /// <summary>
    /// Indicates delivery and read status.
    /// </summary>
    /// <param name="input">The log input data.</param>
    void MessageDelivery(LogInput input);

    /// <summary>
    /// Captures missing data or high processing time.
    /// </summary>
    /// <param name="input">The log input data.</param>
    void MissingInfoLatency(LogInput input);

    /// <summary>
    /// Captures critical failures during the bot execution.
    /// </summary>
    /// <param name="input">The log input data.</param>
    /// <param name="exception">The associated exception that occurred.</param>
    void ErrorEvents(LogInput input, Exception exception);
}
