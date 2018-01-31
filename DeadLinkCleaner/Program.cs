using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using CommandLine;
using EventStore.ClientAPI;

namespace DeadLinkCleaner
{

    public class Program
    {
        public static void Main(string[] args)
        {
            args = new string[]
            {
                "-i", "http://localhost:2113",
                "-s", "$ce-AggregateCmds",
                "-u", "admin",
                "-p", "changeit"
            };

            CommandLine.Parser.Default.ParseArguments<Options>(args)
                .WithParsed(o =>
                {
                    var u = new EventStoreHttpDeadLinkCleanup(o.Uri, o.Username, o.Password);
                    var truncatedAt = u.SafelyTruncateStream(o.Stream).GetAwaiter().GetResult();
                    Console.WriteLine($"{o.Stream} safely truncated at {truncatedAt}.");
                });

            Console.WriteLine("Complete... press any key to exit;");
            Console.ReadKey();
        }


    }
}
