﻿using System;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Server;
using StackExchange.Redis;

namespace Hangfire.Redis.StackExchange
{
#pragma warning disable 618
    internal class RedisSubscription : IServerComponent
#pragma warning restore 618
    {
        private readonly ManualResetEvent _mre = new ManualResetEvent(false);
        private readonly RedisStorage _storage;
        private readonly ISubscriber _subscriber;

        public RedisSubscription([NotNull] RedisStorage storage, [NotNull] ISubscriber subscriber)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            Channel = _storage.GetRedisKey("JobFetchChannel");

            _subscriber = subscriber ?? throw new ArgumentNullException(nameof(subscriber));
            _subscriber.Subscribe(Channel, (channel, value) => _mre.Set());
        }

        public string Channel { get; }

        public void WaitForJob(TimeSpan timeout, CancellationToken cancellationToken)
        {
            _mre.Reset();
            WaitHandle.WaitAny(new[] {_mre, cancellationToken.WaitHandle}, timeout);
        }

        void IServerComponent.Execute(CancellationToken cancellationToken)
        {
            cancellationToken.WaitHandle.WaitOne();

            if (cancellationToken.IsCancellationRequested)
            {
                _subscriber.Unsubscribe(Channel);
                _mre.Dispose();
            }
        }
    }
}