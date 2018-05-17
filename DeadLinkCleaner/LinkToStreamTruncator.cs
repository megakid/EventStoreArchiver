using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;
using DeadLinkCleaner.EventStore.PersistentSubscriptions;
using DeadLinkCleaner.EventStore.PersistentSubscriptions.Internals;
using DeadLinkCleaner.EventStore.Projections;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Projections;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DeadLinkCleaner
{
    public class LinkToStreamTruncator : EventStoreBase
    {
        public LinkToStreamTruncator(string connectionString)
            : base(connectionString)
        {
        }

        public async Task<long?> FindSafeTruncationBeforePoint(string stream,
            long startAtInclusive = StreamPosition.Start)
        {
            if (string.IsNullOrEmpty(stream))
                throw new ArgumentException();

            // Only allow on $et and $ce streams.
            if (!stream.StartsWith("$et-") && !stream.StartsWith("$ce-"))
                return null;

            var esc = await EventStoreConnection;


            //
            // check persistent subs
            //
            var checkpoint = await GetPersistentSubscriptionsCheckpoint(stream);

            if (checkpoint == Checkpoint.Unknown)
                return null;

            //
            // check projections
            //
            var pm = await ProjectionsManager;

            //
            // User continuous projections first...
            //
            checkpoint = await GetUserProjectionsCheckpoint(stream, checkpoint);
            
            if (checkpoint == Checkpoint.Unknown)
                return null;
            
            var endAtExclusive = checkpoint == Checkpoint.NotApplicable
                ? StreamPosition.End
                : ((DirectCheckpoint) checkpoint).Checkpoint;
            
            //
            // Last dead link...
            //
            var nextDeadLink = await GetNextDeadLink(stream, startAtInclusive, endAtExclusive, esc);

            if (!nextDeadLink.HasValue)
                return null;

            // Now read the event we think is safe...
            var candidateEventNumber = nextDeadLink.Value;

            // Invalid - we should only move forwards from start point or backwards from end point...
            if (candidateEventNumber < startAtInclusive)
                return null;
            if (endAtExclusive != StreamPosition.End && candidateEventNumber >= endAtExclusive)
                return null;

            var linkEvents = await esc.ReadStreamEventsForwardAsync(stream, candidateEventNumber, 1, true);

            if (linkEvents.Status != SliceReadStatus.Success || linkEvents.Events?.Length != 1 ||
                linkEvents.Events?[0].OriginalEvent == null)
                return null;

            var linkEvent = linkEvents.Events[0];
            
            // Should always be deletable.
            if (!IsDeletable(linkEvent))
                return null;
            
            //
            // Now system projections ...
            //
            var linkData = linkEvent.Link;
            
            var linkMetadata = JObject
                .Parse(Encoding.UTF8.GetString(linkData.Metadata))
                .ToObject<LinkMetadata>();

            if (!await AreSystemProjectionsPastTruncationPoint(linkMetadata.PreparePosition, linkMetadata.CommitPosition)) 
                return null;
            
            return candidateEventNumber;
        }

        private async Task<bool> AreSystemProjectionsPastTruncationPoint(long prepare, long commit)
        {
            var esc = await EventStoreConnection;
            
            var pm = await ProjectionsManager;
            
            var projections = await pm.ListContinuousAsync();

            foreach (var pd in projections)
            {
                var checkpoint = await GetProjectionCheckpoint(null, pd, esc);

                // since this iterates all projections, ignore those that aren't logically checkpointed.
                if (!(checkpoint is LogicalCheckpoint logicalCheckpoint)) 
                    continue;
                
                if (checkpoint == Checkpoint.Unknown)
                    return false;
                
                if (logicalCheckpoint.PreparePosition < prepare || logicalCheckpoint.CommitPosition < commit)
                    return false;
            }

            return true;
        }

        private async Task<Checkpoint> GetProjectionCheckpoint(string stream,
            ProjectionDetails projectionDetails, IEventStoreConnection esc)
        {
            var n = new CheckpointUser.Projection(projectionDetails.Name);

            return await n.GetCheckpointForStream(esc, stream);
        }


        public class LinkMetadata
        {
            [JsonProperty("$v")] public string V { get; set; }
                
            [JsonProperty("$c")] public long CommitPosition { get; set; }

            [JsonProperty("$p")] public long PreparePosition { get; set; }
            
            [JsonProperty("$o")] public string OriginStream { get; set; }
            
            [JsonProperty("$causedBy")] public Guid CausedBy { get; set; }
        }

        
        private async Task<Checkpoint> GetUserProjectionsCheckpoint(string stream, Checkpoint minDirectCheckpoint)
        {
            var esc = await EventStoreConnection;
            
            var pm = await ProjectionsManager;
            
            var projections = await pm.ListContinuousAsync();

            foreach (var pd in projections)
            {
                // skip system projections
                if (pd.Name.StartsWith("$"))
                    continue;
                
                var relevent = await IsProjectionRelevent(stream, pd, pm);

                if (!relevent)
                    continue;

                var checkpoint = await GetProjectionCheckpoint(stream, pd, esc);

                minDirectCheckpoint = new[] {minDirectCheckpoint, checkpoint}.Min();
            }

            return minDirectCheckpoint;
        }

        private async Task<Checkpoint> GetPersistentSubscriptionsCheckpoint(string stream)
        {
            var esc = await EventStoreConnection;
            
            var psm = await PersistentSubscriptionsManager;

            var persistentSubscriptions = await psm.List(stream);

            // Start with checkpoint not applicable...
            var minDirectCheckpoint = Checkpoint.NotApplicable;

            foreach (var psd in persistentSubscriptions)
            {
                var checkpoint = await GetPersistentSubscriptionsCheckpoint(stream, psd, esc);

                minDirectCheckpoint = new[] {minDirectCheckpoint, checkpoint}.Min();
            }

            return minDirectCheckpoint;
        }

        private static async Task<bool> IsProjectionRelevent(string stream, ProjectionDetails pd,
            IProjectionsManager pm)
        {
            var query = await pm.GetQueryAsync(pd.Name);

            // $ce-X or $et-X => X
            if (stream.StartsWith("$"))
                stream = stream.Substring(stream.IndexOf("-", StringComparison.Ordinal) + 1);

            return query.Contains(stream);
        }

        private static async Task<Checkpoint> GetPersistentSubscriptionsCheckpoint(
            string stream,
            PersistentSubscriptionDetails persistentSubscriptionDetails,
            IEventStoreConnection esc)
        {
            var n = new CheckpointUser.PersistentSubscription(persistentSubscriptionDetails.GroupName);

            return await n.GetCheckpointForStream(esc, stream);
        }


        /// <summary>
        /// Get the next dead link immediately before a live link.  Excludes $metadata.
        /// </summary>
        /// <param name="streamId"></param>
        /// <param name="startAtInclusive"></param>
        /// <param name="endAtExclusive"></param>
        /// <param name="esc"></param>
        /// <returns></returns>
        private static async Task<long?> GetNextDeadLink(string streamId,
            long startAtInclusive,
            long endAtExclusive,
            IEventStoreConnection esc)
        {
            if (endAtExclusive == StreamPosition.End)
                endAtExclusive = long.MaxValue;

            long? deadLinkBeforeLiveLink = null;

            StreamEventsSlice currentSlice;

            var nextSliceStart = startAtInclusive;
            do
            {
                const int readSize = 4096;

                var toGo = endAtExclusive - nextSliceStart;
                var count = (int) Math.Min(readSize, toGo);
                if (count <= 0)
                    break;
                
                currentSlice =
                    await esc.ReadStreamEventsForwardAsync(streamId, nextSliceStart, count, true);

                nextSliceStart = currentSlice.NextEventNumber;

                foreach (var re in currentSlice.Events)
                {
                    // dead link - could be a permissions issue also so need to be running as admin.
                    if (IsDeletable(re))
                        deadLinkBeforeLiveLink = re.OriginalEventNumber;
                    else
                        return deadLinkBeforeLiveLink;
                }
            } while (!currentSlice.IsEndOfStream);

            return deadLinkBeforeLiveLink;
        }

        private static bool IsDeletable(ResolvedEvent re)
        {
            return !re.IsResolved || re.Event.EventType == "$metadata";
        }
    }
    
    
}