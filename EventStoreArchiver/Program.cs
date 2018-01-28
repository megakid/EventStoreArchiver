using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CommandLine;
using EventStore.ClientAPI;

namespace EventStoreArchiver
{
    class Options
    {
        [Option('s', "source", Required = true,
            HelpText = "Source EventStore connection string.")]
        public string Source { get; set; }

        [Option('d', "destination", Required = false,
            HelpText = "Destination EventStore connection string.  If excluded, dry run occurs.")]
        public string Destination { get; set; }

        [Option('p', "streamPatterns", Required = true,
            HelpText = "Regex patterns - stream names must match one to be copied across.")]
        public IEnumerable<string> InputFiles { get; set; }

        // Omitting long name, default --verbose
        [Option(
            HelpText = "Prints all messages to standard output.")]
        public bool Verbose { get; set; }

    }

    public class Program
    {
        public static async Task<int> Main(string[] args)
        {
            // enumerate all streams
            IEventStoreConnection conn = EventStoreConnection.Create(connection);

            await Task.Run(() => conn.ConnectAsync());

            try
            {
                await conn.CreatePersistentSubscriptionAsync(stream, group,
                    PersistentSubscriptionSettings.Create(), null);
            }
            catch
            {
            }

            dynamic ps = conn.ConnectToPersistentSubscription(stream, group,
                (s, e) => Process(s, e),
                (s, r, ex) => HandleException(s, r, ex),
                null,
                2,
                false);

            Console.ReadKey();

        }
    }
}
