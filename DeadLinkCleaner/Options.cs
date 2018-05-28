using System.Security.Cryptography;
using CommandLine;
using EventStore.ClientAPI;

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

        [Option('v', "version", Required = false, 
            Default = StreamPosition.Start,
            HelpText = "Start searching at version")]
        public long StartAtVersion { get; set; }
        
        [Option('f', "force", Required = false,
            Default = false,
            HelpText = "Set stream metadata afterwards")]
        public bool SetTruncatePoint { get; set; }
        
        [Option('g', "GetCurrentTruncationPoint", Required = false,
            Default = false,
            HelpText = "Get stream metadata truncation point")]
        public bool GetTruncatePoint { get; set; } 
        
        [Option('x', "ClearTruncationPoint", Required = false,
            Default = false,
            HelpText = "Clear stream metadata truncation point")]
        public bool ClearTruncatePoint { get; set; }
    }
}
