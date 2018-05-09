using DeadLinkCleaner.EventStore.PersistentSubscriptions;
using DeadLinkCleaner.EventStore.Projections;
using EventStore.ClientAPI;

namespace DeadLinkCleaner
{
    public abstract class EventStoreBase
    {
        protected AsyncLazy<IEventStoreConnection> EventStoreConnection { get; }
        protected AsyncLazy<IProjectionsManager> ProjectionsManager { get; }
        protected AsyncLazy<IPersistentSubscriptionsManager> PersistentSubscriptionsManager { get; }

        public EventStoreBase(string connectionString)
        {
            EventStoreConnection = new AsyncLazy<IEventStoreConnection>(async () =>
            {
                var eventStoreConnection = global::EventStore.ClientAPI.EventStoreConnection.Create(connectionString);
                await eventStoreConnection.ConnectAsync();
                return eventStoreConnection;
            });
            ProjectionsManager =
                new AsyncLazy<IProjectionsManager>(() => EventStoreProjectionsManager.Create(connectionString));
            PersistentSubscriptionsManager = new AsyncLazy<IPersistentSubscriptionsManager>(() =>
                EventStorePersistentSubscriptionsManager.Create(connectionString));
        }
    }
}