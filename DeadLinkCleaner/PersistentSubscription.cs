namespace DeadLinkCleaner
{
    class PersistentSubscription
    {
        public PersistentSubscription(string stream, string @group, long? checkpoint)
        {
            Stream = stream;
            Group = @group;
            Checkpoint = checkpoint;
        }
        public string Stream { get; }
        public string Group { get; }
        public long? Checkpoint { get; }
    }
}