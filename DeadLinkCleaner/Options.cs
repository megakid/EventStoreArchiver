using CommandLine;

namespace DeadLinkCleaner
{
    public class Options
    {
        [Option('i', "url", Required = true,
            HelpText = "EventStore HTTP URI string.")]
        public string Uri { get; set; }

        [Option('u', "user", HelpText = "Username")]
        public string Username { get; set; }
        [Option('p', "pass", HelpText = "Password")]
        public string Password { get; set; }

        [Option('s', "stream", Required = true,
            HelpText = "The stream to truncate")]
        public string Stream { get; set; }

    }
}
