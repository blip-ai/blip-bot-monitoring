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
    /// Logs events related to message processing by the bot.
    /// </summary>
    /// <param name="input">The log input data.</param>
    void MessageProcessing(LogInput input);

    /// <summary>
    /// Logs the execution of an action within the bot flow.
    /// </summary>
    /// <param name="input">The log input data.</param>
    void ActionExecution(LogInput input);

    /// <summary>
    /// Logs the user context related to the conversation.
    /// </summary>
    /// <param name="input">The log input data.</param>
    void UserContext(LogInput input);

    /// <summary>
    /// Logs information about the conversation flow or state transitions.
    /// </summary>
    /// <param name="input">The log input data.</param>
    void ConversationalFlow(LogInput input);

    /// <summary>
    /// Logs raw input messages sent by the user.
    /// </summary>
    /// <param name="input">The log input data.</param>
    void UserInput(LogInput input);

    /// <summary>
    /// Logs details related to the message delivery process.
    /// </summary>
    /// <param name="input">The log input data.</param>
    void MessageDelivery(LogInput input);

    /// <summary>
    /// Logs latency caused by missing information or delayed user input.
    /// </summary>
    /// <param name="input">The log input data.</param>
    void MissingInfoLatency(LogInput input);

    /// <summary>
    /// Logs error events that include exceptions or critical failures.
    /// </summary>
    /// <param name="input">The log input data.</param>
    /// <param name="exception">The associated exception that occurred.</param>
    void ErrorEvents(LogInput input, Exception exception);
}
