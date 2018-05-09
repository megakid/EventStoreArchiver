using System;
using System.Runtime.Serialization;
using EventStore.ClientAPI.Exceptions;

namespace DeadLinkCleaner.EventStore.PersistentSubscriptions
{
    /// <summary>
         /// Exception thrown if a persistent subscription command fails.
         /// </summary>
         public class PersistentSubscriptionCommandFailedException : EventStoreConnectionException
         {
             /// <summary>
             /// The Http status code returned by the server
             /// </summary>
             public int HttpStatusCode { get; private set; }
     
             /// <summary>
             /// Constructs a new <see cref="PersistentSubscriptionCommandFailedException"/>.
             /// </summary>
             public PersistentSubscriptionCommandFailedException()
             {
             }
     
             /// <summary>
             /// Constructs a new <see cref="PersistentSubscriptionCommandFailedException"/>.
             /// </summary>
             public PersistentSubscriptionCommandFailedException(int httpStatusCode, string message)
                 : base(message)
             {
                 HttpStatusCode = httpStatusCode;
             }
     
             /// <summary>
             /// Constructs a new <see cref="PersistentSubscriptionCommandFailedException"/>.
             /// </summary>
             public PersistentSubscriptionCommandFailedException(string message,
                 Exception innerException)
                 : base(message, innerException)
             {
             }
         }
}