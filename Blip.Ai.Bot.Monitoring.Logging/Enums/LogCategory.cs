namespace Blip.Ai.Bot.Monitoring.Logging.Enums;

/// <summary>
/// Defines categories used for bot logging.
/// </summary>
public enum LogCategory
{
    /// <summary>
    /// Captures how the bot interprets and processes user input,
    /// including detected intents, entities, and fallbacks.
    /// </summary>
    MessageProcessing,

    /// <summary>
    /// Logs actions performed by the bot such as API calls,
    /// script executions, and message dispatches.
    /// </summary>
    ActionExecution,

    /// <summary>
    /// Records contextual information about the user,
    /// such as user ID, custom attributes, and session data.
    /// </summary>
    UserContext,

    /// <summary>
    /// Tracks user navigation through the conversation flow,
    /// including block entries, state transitions, and time spent.
    /// </summary>
    ConversationalFlow,

    /// <summary>
    /// Logs raw input from the user including text, button clicks,
    /// and other interactive elements like carousels.
    /// </summary>
    UserInput,

    /// <summary>
    /// Indicates message delivery and read status,
    /// including delivery confirmations and errors.
    /// </summary>
    MessageDelivery,

    /// <summary>
    /// Captures cases of missing information or latency issues
    /// during message processing or action execution.
    /// </summary>
    MissingInfoLatency,

    /// <summary>
    /// Logs critical failures during bot execution,
    /// such as API timeouts, script errors, or intent recognition failures.
    /// </summary>
    ErrorEvents,
}
