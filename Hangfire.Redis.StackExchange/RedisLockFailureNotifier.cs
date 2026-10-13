using System;
using System.Collections.Concurrent;
using System.Threading;
using Hangfire.Logging;
using Hangfire.States;

namespace Hangfire.Redis.StackExchange
{
    // Hangfire exposes a synchronous state API. Keep it off the pool that completes Redis I/O.
    // A single background thread also bounds concurrent state publications during mass lease loss.
    internal static class RedisLockFailureNotifier
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(RedisLockFailureNotifier));
        private static readonly BlockingCollection<Action> Notifications = new BlockingCollection<Action>(1024);

        static RedisLockFailureNotifier()
        {
            var thread = new Thread(() =>
            {
                foreach (var notify in Notifications.GetConsumingEnumerable())
                {
                    try { notify(); }
                    catch (Exception ex) { Logger.ErrorException("Unable to record distributed lock renewal failure.", ex); }
                }
            }) { IsBackground = true, Name = "Hangfire.Redis.LockFailure" };
            using (ExecutionContext.SuppressFlow()) thread.Start();
        }

        internal static void TryNotify(JobStorage storage, string jobId, Exception error, Func<bool> isDisposed)
        {
            if (!Notifications.TryAdd(() =>
                {
                    if (!isDisposed())
                        new BackgroundJobClient(storage).ChangeState(jobId, new FailedState(error), ProcessingState.StateName);
                }))
                Logger.Error("Distributed lock failure notification queue is full; no job was replayed.");
        }
    }
}
