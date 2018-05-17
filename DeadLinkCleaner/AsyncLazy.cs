using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace DeadLinkCleaner
{
    public class AsyncLazy<T> : Lazy<ValueTask<T>>
    {
        public AsyncLazy(Func<T> valueFactory) :
            base(() => new ValueTask<T>(valueFactory())) { }

        public AsyncLazy(Func<Task<T>> valueFactory) :
            base(() => new ValueTask<T>(valueFactory())) { }

        public ValueTaskAwaiter<T> GetAwaiter() { return Value.GetAwaiter(); }
    }
}