namespace Blip.Ai.Bot.Monitoring.Logging.Models
{
    /// <summary>
    /// Represents configuration options for connecting to a Firehose endpoint.
    /// </summary>
    public class FireHoseOptions
    {
        /// <summary>
        /// Gets or sets the Firehose's HTTP URL endpoint.
        /// </summary>
        public string? Address { get; set; }

        /// <summary>
        /// Gets or sets the username for Firehose authentication.
        /// </summary>
        public string? UserName { get; set; }

        /// <summary>
        /// Gets or sets the password for Firehose authentication.
        /// </summary>
        public string? Password { get; set; }

        /// <summary>
        /// Gets or sets the URL authentication string for the Firehose endpoint.
        /// </summary>
        public string? UrlAuthentication { get; set; }

        /// <summary>
        /// Gets or sets the URL used to refresh the authentication token for the Firehose endpoint.
        /// </summary>
        public string? UrlRefreshToken { get; set; }

        /// <summary>
        /// Determines whether the current FireHoseOptions instance has valid configuration.
        /// </summary>
        /// <returns>
        /// <c>true</c> if all required properties are set; otherwise, <c>false</c>.
        /// </returns>
        public bool IsValid()
        {
            return !string.IsNullOrEmpty(Address)
                && !string.IsNullOrEmpty(UserName)
                && !string.IsNullOrEmpty(Password)
                && !string.IsNullOrEmpty(UrlAuthentication);
        }
    }
}
