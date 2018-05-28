using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;

namespace DeadLinkCleaner
{
    abstract class Checkpoint : IComparable<Checkpoint>, IComparable
    {
        public static readonly Checkpoint NotApplicable = NotApplicableCheckpoint.Instance;
        public static readonly Checkpoint Unknown = UnknownCheckpoint.Instance;

        public int CompareTo(Checkpoint other)
        {
            switch (this)
            {
                case DirectCheckpoint dc when other is DirectCheckpoint odc:
                    return dc.CompareTo(odc);                
                
                case LogicalCheckpoint lc when other is LogicalCheckpoint olc:
                    return lc.CompareTo(olc);

                case NotApplicableCheckpoint nac when other is NotApplicableCheckpoint onac:
                    return 0;
                case NotApplicableCheckpoint nac:
                    return 1;
                case Checkpoint _ when other is NotApplicableCheckpoint onac:
                    return -1;

                case UnknownCheckpoint uc when other is UnknownCheckpoint ouc:
                    return 0;
                case UnknownCheckpoint uc:
                    return -1;
                case Checkpoint _ when other is UnknownCheckpoint uc:
                    return 1;

                default:
                    throw new InvalidOperationException();
            }
        }

        public int CompareTo(object obj)
        {
            if (ReferenceEquals(null, obj)) return 1;
            if (ReferenceEquals(this, obj)) return 0;
            if (!(obj is Checkpoint)) throw new ArgumentException($"Object must be of type {nameof(Checkpoint)}");
            return CompareTo((Checkpoint) obj);
        }


        public static bool operator <(Checkpoint left, Checkpoint right)
        {
            return Comparer<Checkpoint>.Default.Compare(left, right) < 0;
        }

        public static bool operator >(Checkpoint left, Checkpoint right)
        {
            return Comparer<Checkpoint>.Default.Compare(left, right) > 0;
        }

        public static bool operator <=(Checkpoint left, Checkpoint right)
        {
            return Comparer<Checkpoint>.Default.Compare(left, right) <= 0;
        }

        public static bool operator >=(Checkpoint left, Checkpoint right)
        {
            return Comparer<Checkpoint>.Default.Compare(left, right) >= 0;
        }
    }

    class NotApplicableCheckpoint : Checkpoint, IEquatable<NotApplicableCheckpoint>
    {
        public static readonly NotApplicableCheckpoint Instance = new NotApplicableCheckpoint();

        private NotApplicableCheckpoint()
        {
        }

        public bool Equals(NotApplicableCheckpoint other)
        {
            return true;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((NotApplicableCheckpoint) obj);
        }

        public override int GetHashCode()
        {
            return 1;
        }

        public static bool operator ==(NotApplicableCheckpoint left, NotApplicableCheckpoint right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(NotApplicableCheckpoint left, NotApplicableCheckpoint right)
        {
            return !Equals(left, right);
        }

        public override string ToString()
        {
            return $"[Not Applicable]";
        }
    }

    class UnknownCheckpoint : Checkpoint, IEquatable<UnknownCheckpoint>
    {
        public static readonly UnknownCheckpoint Instance = new UnknownCheckpoint();

        private UnknownCheckpoint()
        {
        }

        public bool Equals(UnknownCheckpoint other)
        {
            return true;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((UnknownCheckpoint) obj);
        }

        public override int GetHashCode()
        {
            return 1;
        }

        public static bool operator ==(UnknownCheckpoint left, UnknownCheckpoint right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(UnknownCheckpoint left, UnknownCheckpoint right)
        {
            return !Equals(left, right);
        }
        public override string ToString()
        {
            return $"[Unknown]";
        }
    }

    class DirectCheckpoint : Checkpoint, IComparable<DirectCheckpoint>
    {
        public DirectCheckpoint(long checkpoint)
        {
            Checkpoint = checkpoint;
        }

        public long Checkpoint { get; }

        public int CompareTo(DirectCheckpoint other)
        {
            if (ReferenceEquals(this, other)) return 0;
            if (ReferenceEquals(null, other)) return 1;
            return Checkpoint.CompareTo(other.Checkpoint);
        }

        public static bool operator <(DirectCheckpoint left, DirectCheckpoint right)
        {
            return Comparer<DirectCheckpoint>.Default.Compare(left, right) < 0;
        }

        public static bool operator >(DirectCheckpoint left, DirectCheckpoint right)
        {
            return Comparer<DirectCheckpoint>.Default.Compare(left, right) > 0;
        }

        public static bool operator <=(DirectCheckpoint left, DirectCheckpoint right)
        {
            return Comparer<DirectCheckpoint>.Default.Compare(left, right) <= 0;
        }

        public static bool operator >=(DirectCheckpoint left, DirectCheckpoint right)
        {
            return Comparer<DirectCheckpoint>.Default.Compare(left, right) >= 0;
        }
        
        
        public override string ToString()
        {
            return $"[Direct {Checkpoint}]";
        }
    }

    abstract class CheckpointUser
    {
        public string Name { get; protected set; }

        public abstract string CheckpointStream { get; }

        public abstract Task<Checkpoint> GetCheckpointForStream(IEventStoreConnection eventStoreConnection,
            string stream);

        public class PersistentSubscription : CheckpointUser
        {
            public override string CheckpointStream => $"$persistentsubscription-{{0}}::{Name}-checkpoint";

            public PersistentSubscription(string groupName)
            {
                Name = groupName;
            }

            public override async Task<Checkpoint> GetCheckpointForStream(IEventStoreConnection eventStoreConnection,
                string stream)
            {
                var checkpointStream = string.Format(CheckpointStream, stream);

                var headEvent = await eventStoreConnection.ReadEventAsync(checkpointStream, StreamPosition.End, false);

                if (headEvent.Status != EventReadStatus.Success || headEvent.Event?.OriginalEvent == null)
                {
                    Log.Information("Could not load checkpoint event for {checkpointStream}.", checkpointStream);
                    // Safer to return zero -
                    // it is assumed that this PS is relevant on the stream given
                    // so in that case a checkpoint hasn't been written yet.
                    return Checkpoint.Unknown;
                }

                var resolvedEvent = headEvent.Event.Value;

                return new DirectCheckpoint(long.Parse(Encoding.UTF8.GetString(resolvedEvent.OriginalEvent.Data)));
            }
        }


        public class Projection : CheckpointUser
        {
            public override string CheckpointStream => $"$projections-{Name}-checkpoint";

            public Projection(string projectionName)
            {
                Name = projectionName;
            }


            public class ProjectionCheckpointMetadata
            {
                [JsonProperty("$v")] public string V { get; set; }

                [JsonProperty("$s")] public IDictionary<string, long> StreamCheckpoints { get; set; }
                
                [JsonProperty("$c")] public long? C { get; set; }

                [JsonProperty("$p")] public long? P { get; set; }
            }
            
            public override async Task<Checkpoint> GetCheckpointForStream(IEventStoreConnection eventStoreConnection,
                string stream)
            {
                try
                {
                    var headEvent =
                        await eventStoreConnection.ReadEventAsync(CheckpointStream, StreamPosition.End, false);

                    if (headEvent.Status != EventReadStatus.Success || headEvent.Event?.OriginalEvent == null)
                    {
                        Log.Warning("Could not load checkpoint event for {checkpointStream}.", CheckpointStream);

                        // Safer to return zero -
                        // it is assumed that this PS is relevant on the stream given
                        // so in that case a checkpoint hasn't been written yet.
                        return Checkpoint.Unknown;
                    }

                    var resolvedEvent = headEvent.Event?.OriginalEvent;

                    var metaDataJson = JObject
                        .Parse(Encoding.UTF8.GetString(resolvedEvent.Metadata))
                        .ToObject<ProjectionCheckpointMetadata>();

                    if (metaDataJson.C.HasValue && metaDataJson.P.HasValue)
                        return new LogicalCheckpoint(metaDataJson.C.Value, metaDataJson.P.Value);

                    if (metaDataJson.StreamCheckpoints != null)
                    {
                        if (metaDataJson.StreamCheckpoints.TryGetValue(stream, out var o))
                            return new DirectCheckpoint(o);

                        return Checkpoint.NotApplicable;
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error loading checkpoint for {checkpointStream}", CheckpointStream);
                    // return Unknown
                }

                return Checkpoint.Unknown;
            }
        }
    }

    internal class LogicalCheckpoint : Checkpoint, IComparable<LogicalCheckpoint>
    {
        public long CommitPosition { get; }
        public long PreparePosition { get; }

        public LogicalCheckpoint(long commitPosition, long preparePosition)
        {
            CommitPosition = commitPosition;
            PreparePosition = preparePosition;
        }

        public int CompareTo(LogicalCheckpoint other)
        {
            if (ReferenceEquals(this, other)) return 0;
            if (ReferenceEquals(null, other)) return 1;
            var commitPositionComparison = CommitPosition.CompareTo(other.CommitPosition);
            if (commitPositionComparison != 0) return commitPositionComparison;
            return PreparePosition.CompareTo(other.PreparePosition);
        }

        public static bool operator <(LogicalCheckpoint left, LogicalCheckpoint right)
        {
            return Comparer<LogicalCheckpoint>.Default.Compare(left, right) < 0;
        }

        public static bool operator >(LogicalCheckpoint left, LogicalCheckpoint right)
        {
            return Comparer<LogicalCheckpoint>.Default.Compare(left, right) > 0;
        }

        public static bool operator <=(LogicalCheckpoint left, LogicalCheckpoint right)
        {
            return Comparer<LogicalCheckpoint>.Default.Compare(left, right) <= 0;
        }

        public static bool operator >=(LogicalCheckpoint left, LogicalCheckpoint right)
        {
            return Comparer<LogicalCheckpoint>.Default.Compare(left, right) >= 0;
        }
        public override string ToString()
        {
            return $"[Position {PreparePosition}/{CommitPosition}]";
        }
    }
}