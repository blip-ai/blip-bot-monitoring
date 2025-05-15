using Blip.Ai.Bot.Monitoring.Logging.Models;

namespace Blip.Ai.Bot.Monitoring.Logging.Interface;

public interface IBlipLogger
{
    void MessageProcessing(LogInput input);
    void ActionExecution(LogInput input);
    void UserContext(LogInput input);
    void ConversationalFlow(LogInput input);
    void UserInput(LogInput input);
    void MessageDelivery(LogInput input);
    void MissingInfoLatency(LogInput input);
    void ErrorEvents(LogInput input, Exception ex);
}