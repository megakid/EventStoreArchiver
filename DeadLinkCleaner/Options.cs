using CommandLine;

namespace DeadLinkCleaner
{
    public class Options
    {
        [Option('c', "connection", Required = true,
            HelpText = "EventStore connection string.")]
        public string ConnectionString { get; set; }

        [Option('s', "stream", Required = true,
            HelpText = "The stream to truncate")]
        public string Stream { get; set; }

    }
}
