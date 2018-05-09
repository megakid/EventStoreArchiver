using System;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading.Tasks;
using DeadLinkCleaner.EventStore.PersistentSubscriptions;
using DeadLinkCleaner.EventStore.PersistentSubscriptions.Internals;
using DeadLinkCleaner.EventStore.Projections;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Projections;
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
            var psm = await PersistentSubscriptionsManager;

            var persistentSubscriptions = await psm.List(stream);

            // Start with checkpoint not applicable...
            Checkpoint minDirectCheckpoint = Checkpoint.NotApplicable;

            foreach (var psd in persistentSubscriptions)
            {
                var checkpoint = await GetPersistentSubscriptionCheckpoint(stream, psd, esc);

                if (checkpoint == Checkpoint.Unknown)
                    return null;

                if (checkpoint == Checkpoint.NotApplicable)
                    continue;

                minDirectCheckpoint = new[] {minDirectCheckpoint, checkpoint}.Min();
            }


            //
            // check projections
            //
            var pm = await ProjectionsManager;

            //
            // User continuous projections first...
            //
            var projections = await pm.ListContinuousAsync();

            foreach (var pd in projections.Where(pd => !pd.Name.StartsWith("$")))
            {
                var relevent = await IsProjectionRelevent(stream, pd, pm);

                if (!relevent)
                    continue;

                var checkpoint = await GetCustomProjectionCheckpoint(stream, pd, esc);

                if (checkpoint == Checkpoint.Unknown)
                    return null;

                if (checkpoint == Checkpoint.NotApplicable)
                    continue;

                minDirectCheckpoint = new[] {minDirectCheckpoint, checkpoint}.Min();
            }

            var endAtExclusive = minDirectCheckpoint == Checkpoint.NotApplicable
                ? StreamPosition.End
                : ((DirectCheckpoint) minDirectCheckpoint).Checkpoint;

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

            return candidateEventNumber;
        }

        private static async Task<bool> IsProjectionRelevent(string stream, ProjectionDetails pd,
            IProjectionsManager pm)
        {
            var query = await pm.GetQueryAsync(pd.Name);

            if (stream.StartsWith("$"))
                stream = stream.Substring(stream.IndexOf("-", StringComparison.Ordinal) + 1);

            return query.Contains(stream);
        }

        private static async Task<Checkpoint> GetCustomProjectionCheckpoint(
            string stream, ProjectionDetails projectionDetails, IEventStoreConnection esc)
        {
            var n = new CheckpointUser.Projection(projectionDetails.Name);

            return await n.GetCheckpointForStream(esc, stream);
        }

        private static async Task<Checkpoint> GetPersistentSubscriptionCheckpoint(
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