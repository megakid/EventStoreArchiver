using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using DeadLinkCleaner.EventStore.PersistentSubscriptions;
using DeadLinkCleaner.EventStore.PersistentSubscriptions.Internals;
using DeadLinkCleaner.EventStore.Projections;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Projections;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;

namespace DeadLinkCleaner
{
    public class LinkToStreamTruncator : EventStoreBase
    {
        /*
fromAll()
.when({
    $init:function(){
        return { Categories: {}, EventTypes: {} };
    },
    $any: function(s,e){
        
        if (e.sequenceNumber == 0) {
            var i = e.streamId.indexOf('-');
            if (i > 0) {
                var category = e.streamId.substr(0, i);
                s.Categories[category] = true;
            }
        }
        
        s.EventTypes[e.eventType] = true;
        return s;
    }
})
         */
        public LinkToStreamTruncator(string connectionString)
            : base(connectionString)
        {
        }

        public async Task<long?> FindSafeTruncationBeforePoint(string stream,
            long startAtInclusive = StreamPosition.Start,
            bool set = false)
        {
            var currentTruncatePoint = await GetTruncateBefore(stream);

            if (currentTruncatePoint.HasValue)
            {
                startAtInclusive = Math.Max(startAtInclusive, currentTruncatePoint.Value);
            }
            
            Log.Information("Trying to find safe truncation point of {stream}, starting at {startAtInclusive}.", stream, startAtInclusive);

            // Only allow on $et and $ce streams.
            if (!stream.StartsWith("$et-") && !stream.StartsWith("$ce-"))
            {
                Log.Error("Stream must be $et- or $ce-");
                return null;
            }

            var esc = await EventStoreConnection;


            //
            // check persistent subs
            //
            var checkpoint = await GetPersistentSubscriptionsCheckpoint(stream);

            if (checkpoint == Checkpoint.Unknown)
            {
                Log.Warning("Persistent Subscriptons checkpoints on {stream} are unknown. Aborting...", stream);

                return null;
            }

            //
            // check projections
            //
            var pm = await ProjectionsManager;

            //
            // User continuous projections first...
            //
            checkpoint = await GetUserProjectionsCheckpoint(stream, checkpoint);

            if (checkpoint == Checkpoint.Unknown)
            {
                Log.Warning("User Projection checkpoints on {stream} are unknown. Aborting...", stream);

                return null;
            }

            var endAtExclusive = checkpoint == Checkpoint.NotApplicable
                ? StreamPosition.End
                : ((DirectCheckpoint) checkpoint).Checkpoint;

            Log.Information("Considering truncation points on {stream} up to [{endAtExclusive}], finding last dead link...", stream, endAtExclusive);


            //
            // Last dead link...
            //
            var nextDeadLink = await GetNextDeadLink(stream, startAtInclusive, endAtExclusive, esc);

            Log.Information("Last dead link on {stream} is [{nextDeadLink}]", stream, nextDeadLink);

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

            Log.Information("Found safe truncation point of {stream} as {candidateEventNumber}.", stream, candidateEventNumber);


            
            if (set)
            {
                Log.Warning("Setting truncation point of {stream} from {currentTruncatePoint} to {candidateEventNumber}.", stream, currentTruncatePoint, candidateEventNumber);

                await SetTruncateBefore(stream, candidateEventNumber);
            }
            else
            {
                Log.Information("Current truncation point of {stream} is {currentTruncatePoint}.  Leaving alone due to -f arg not set.", stream, currentTruncatePoint, candidateEventNumber);
            }

            return candidateEventNumber;
        }

        public async Task SetTruncateBefore(string stream, long? truncateBefore)
        {
            var esc = await EventStoreConnection;

            var metadata = await esc.GetStreamMetadataAsync(stream);

            var streamMetadata = metadata.StreamMetadata;

            StreamMetadataBuilder edittedStreamMetadata = streamMetadata.Copy();

            edittedStreamMetadata.SetTruncateBefore(truncateBefore ?? 0);

            await esc.SetStreamMetadataAsync(stream, ExpectedVersion.Any, edittedStreamMetadata.Build());
        }
        
        private async Task<bool> AreSystemProjectionsPastTruncationPoint(long prepare, long commit)
        {
            var esc = await EventStoreConnection;

            var pm = await ProjectionsManager;

            Log.Information("Ensuring system projections are ahead of possible truncation point of [{prepare}/{commit}].", prepare, commit);

            var projections = await pm.ListContinuousAsync();

            foreach (var pd in projections)
            {
                // skip non system projections
                if (!pd.Name.StartsWith("$"))
                    continue;

                var checkpoint = await GetProjectionCheckpoint(null, pd, esc);

                // since this iterates all projections, ignore those that aren't logically checkpointed.
                if (!(checkpoint is LogicalCheckpoint logicalCheckpoint))
                    continue;

                Log.Information("Checkpoint for {projectionName}: {checkpoint}", pd.Name, checkpoint);

                if (checkpoint == Checkpoint.Unknown)
                {
                    Log.Information(
                        "Checkpoint for {projectionName} is unknown, therefore might be behind potential truncation point [{prepare}/{commit}]. Aborting...",
                        pd.Name, prepare, commit);

                    return false;
                }

                if (logicalCheckpoint.PreparePosition < prepare || logicalCheckpoint.CommitPosition < commit)
                {
                    Log.Information("Checkpoint for {projectionName} is behind potential truncation point [{prepare}/{commit}]. Aborting...", pd.Name, prepare,
                        commit);

                    return false;
                }
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

                Log.Information("Checkpoint for {stream} projection {projectionName}: {checkpoint}", stream, pd.Name, checkpoint);

                minDirectCheckpoint = new[] {minDirectCheckpoint, checkpoint}.Min();
            }

            Log.Information("Minimum checkpoint for {stream} projections: {minDirectCheckpoint}", stream, minDirectCheckpoint);


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

                Log.Information("Checkpoint for {stream} subscription {persistentSubscriptionGroup}: {checkpoint}", stream, psd.GroupName, checkpoint);

                minDirectCheckpoint = new[] {minDirectCheckpoint, checkpoint}.Min();
            }

            Log.Information("Minimum checkpoint for {stream} subscriptions: {minDirectCheckpoint}", stream, minDirectCheckpoint);


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
            Log.Information("Searching for last dead link in {stream} from {startAtInclusive}.", streamId,
                (startAtInclusive == StreamPosition.Start ? "stream start" : startAtInclusive.ToString()));

            if (endAtExclusive == StreamPosition.End)
                endAtExclusive = long.MaxValue;

            long? deadLinkBeforeLiveLink = null;

            DateTime utc = DateTime.UtcNow;

            const int readSize = 4096;
            StreamEventsSlice currentSlice;
            Task<StreamEventsSlice> nextSlice;

            long nextSliceStart = startAtInclusive;
            nextSlice = esc.ReadStreamEventsForwardAsync(streamId, nextSliceStart, readSize, true);
            
            do
            {

                /*var toGo = endAtExclusive - nextSliceStart;
                var count = (int) Math.Min(readSize, toGo);
                if (count <= 0)
                    break;*/

                currentSlice = await nextSlice;
                
                nextSliceStart = currentSlice.NextEventNumber;

                if (!currentSlice.IsEndOfStream)
                {
                    nextSlice = esc.ReadStreamEventsForwardAsync(streamId, nextSliceStart, readSize, true);
                }
                
                foreach (var re in currentSlice.Events)
                {
                    if (deadLinkBeforeLiveLink >= endAtExclusive)
                        break;
                    
                    // dead link - could be a permissions issue also so need to be running as admin.
                    if (IsDeletable(re))
                    {
                        deadLinkBeforeLiveLink = re.OriginalEventNumber;
                    }
                    else
                    {
                        Log.Information("Found last dead link at {deadLinkBeforeLiveLink}.", deadLinkBeforeLiveLink);

                        return deadLinkBeforeLiveLink;
                    }
                }
                
                if (DateTime.UtcNow - utc > TimeSpan.FromSeconds(30))
                {
                    Log.Information("  Searching for last dead link from {startAtInclusive} onwards.  Searched to {nextSliceStart}...", startAtInclusive,
                        nextSliceStart);
                    utc = DateTime.UtcNow;
                }
                
            } while (!currentSlice.IsEndOfStream && deadLinkBeforeLiveLink < endAtExclusive);

            return deadLinkBeforeLiveLink;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsDeletable(ResolvedEvent re)
        {
            return !re.IsResolved || re.Event.EventType == "$metadata";
        }

        public async Task<long?> GetTruncateBefore(string stream)
        {
            return (await (await EventStoreConnection).GetStreamMetadataAsync(stream)).StreamMetadata.TruncateBefore;
        }
    }
}