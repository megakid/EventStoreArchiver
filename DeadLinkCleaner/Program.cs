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
                "-c", "ConnectTo=tcp://admin:changeit@localhost:1112; Http=http://admin:changeit@localhost:1113; HeartBeatTimeout=10000; ReconnectionDelay=500; MaxReconnections=-1; MaxDiscoverAttempts=2147483647; VerboseLogging=false",
                "-s", "$ce-AggregateCmds",
            };

            CommandLine.Parser.Default.ParseArguments<Options>(args)
                .WithParsed(o =>
                {
                    var u = new EventStoreTests(o.ConnectionString);
                    
                    u.RunAll().GetAwaiter().GetResult();
                    
//                    var truncatedAt = u.SafelyTruncateStream(o.Stream).GetAwaiter().GetResult();
//                    Console.WriteLine($"{o.Stream} safely truncated at {truncatedAt}.");
                });

            Console.WriteLine("Complete... press any key to exit;");
            Console.ReadKey();
        }


    }
}
