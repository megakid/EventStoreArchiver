using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using CommandLine;
using EventStore.ClientAPI;
using Serilog;

namespace DeadLinkCleaner
{

    public class Program
    {
        public static void Main(string[] args)
        {
//            args = new string[]
//            {
//                "-c", "ConnectTo=tcp://admin:changeit@localhost:1112; Http=http://admin:changeit@localhost:1113; HeartBeatTimeout=10000; ReconnectionDelay=500; MaxReconnections=-1; MaxDiscoverAttempts=2147483647; VerboseLogging=false",
//                "-s", "$ce-AggregateCmds",
//            };
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();


            CommandLine.Parser.Default.ParseArguments<Options>(args)
                .WithParsed(o =>
                {
                    if (string.IsNullOrWhiteSpace(o.ConnectionString))
                        throw new ArgumentException("ConnectionString invalid.");
                    if (string.IsNullOrWhiteSpace(o.Stream))
                        throw new ArgumentException("Stream invalid");
                    
                    //var u = new EventStoreTests(o.ConnectionString);
                    var u = new LinkToStreamTruncator(o.ConnectionString);
                    
                    //u.().GetAwaiter().GetResult();
                    if (o.GetTruncatePoint)
                    {
                        var currentTruncateAt = u.GetTruncateBefore(o.Stream).GetAwaiter().GetResult();
                        Log.Fatal("{stream} current truncate point is {truncateAt}", o.Stream, currentTruncateAt);
                    } 
                    else if (o.ClearTruncatePoint)
                    {
                        var currentTruncateAt = u.GetTruncateBefore(o.Stream).GetAwaiter().GetResult();

                        if (currentTruncateAt.HasValue)
                        {
                            u.SetTruncateBefore(o.Stream, null).GetAwaiter().GetResult();

                            Log.Fatal("{stream} current truncate point was {truncateAt}, cleared it.", o.Stream, currentTruncateAt);
                        }
                        else
                        {
                            Log.Fatal("{stream} current truncate point was {truncateAt}, skipping clearing it...", o.Stream, currentTruncateAt);
                        }
                    }
                    else
                    {
                        var truncatedAt = u.FindSafeTruncationBeforePoint(o.Stream, o.StartAtVersion, o.SetTruncatePoint).GetAwaiter().GetResult();
                        Log.Fatal("{stream} could be safely truncated at {truncatedAt}.", o.Stream, truncatedAt);
                    }
                });

        }


    }
}
