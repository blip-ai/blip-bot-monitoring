namespace Blip.Ai.Bot.Monitoring.Logging.Models
{
    public class FireHoseOptions
    {
        /// <summary>
        /// Firehose configuration ids
        /// </summary>
        public string? Ids { get; set; }

        /// <summary>
        /// Firehose authentication to use on authorization header
        /// </summary>
        public string? Authentication { get; set; }

        /// <summary>
        /// Firehose's http url endpoint
        /// </summary>
        public string? Address { get; set; }

        /// <summary>
        /// Firehose's http path
        /// </summary>
        public string? Path { get; set; }


        public bool IsValid()
        {
            return !string.IsNullOrEmpty(Ids) && !string.IsNullOrEmpty(Authentication) &&
                   !string.IsNullOrEmpty(Address) && !string.IsNullOrEmpty(Path);
        }
    }
}
