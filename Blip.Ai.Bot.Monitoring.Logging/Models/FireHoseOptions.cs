namespace Blip.Ai.Bot.Monitoring.Logging.Models
{
    public class FireHoseOptions
    {
        /// <summary>
        /// Firehose authentication to use on authorization header
        /// </summary>
        public string? Authentication { get; set; }

        /// <summary>
        /// Firehose's http url endpoint
        /// </summary>
        public string? Address { get; set; }

        public bool IsValid()
        {
            return !string.IsNullOrEmpty(Authentication) && !string.IsNullOrEmpty(Address);
        }
    }
}
