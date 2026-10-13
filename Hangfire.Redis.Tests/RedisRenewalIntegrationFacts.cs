using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Hangfire.Redis.StackExchange;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Testcontainers.Redis;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Redis.Tests
{
    [Collection("Sequential")]
    public class RedisRenewalIntegrationFacts
    {
        private readonly ITestOutputHelper _output;
        public RedisRenewalIntegrationFacts(ITestOutputHelper output) { _output = output; }

        [Theory, Trait("Category", "Integration")]
        [InlineData(40)]
        [InlineData(80)]
        public async Task NaturalRenewalsKeepTheWorkerResponsive(int workers)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
            await using var network = new NetworkBuilder().Build();
            await network.CreateAsync(timeout.Token);
            await using var redis = new RedisBuilder("redis:7.4")
                .WithNetwork(network).WithNetworkAliases("redis").Build();
            await redis.StartAsync(timeout.Token);
            using var mux = await ConnectionMultiplexer.ConnectAsync(redis.GetConnectionString());
            var database = mux.GetDatabase();
            // A separate test process preserves default pool settings and avoids contaminating the xUnit host.
            var sdk = Environment.GetEnvironmentVariable("RENEWAL_TEST_SDK_IMAGE") ?? "mcr.microsoft.com/dotnet/sdk:9.0";
            await using var worker = new ContainerBuilder(sdk)
                .WithNetwork(network)
                .WithPortBinding(9090, true)
                .WithResourceMapping(new DirectoryInfo(AppContext.BaseDirectory), "/tests")
                .WithEnvironment("RENEWAL_WORKER_COUNT", workers.ToString())
                .WithEnvironment("RENEWAL_REDIS", "redis:6379")
                .WithEnvironment("DOTNET_ROLL_FORWARD", "Major")
                .WithCreateParameterModifier(parameters =>
                {
                    parameters.HostConfig.NanoCPUs = 2_000_000_000;
                    parameters.HostConfig.Memory = 1000 * 1024 * 1024;
                })
                .WithCommand("dotnet", "vstest", "/tests/Hangfire.Redis.Tests.dll",
                    "--TestCaseFilter:FullyQualifiedName=Hangfire.Redis.Tests.RedisRenewalWorkerFacts.ExecuteJobs")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request =>
                    request.ForPort(9090).ForPath("/metrics")))
                .Build();
            await worker.StartAsync(timeout.Token);
            var exit = worker.GetExitCodeAsync(timeout.Token);
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
            var url = $"http://{worker.Hostname}:{worker.GetMappedPublicPort(9090)}/metrics";
            var sawActiveWork = false;
            var failures = 0;
            while (await database.StringGetAsync("renewal-done") != "yes")
            {
                timeout.Token.ThrowIfCancellationRequested();
                try
                {
                    var active = int.Parse(await http.GetStringAsync(url, timeout.Token));
                    sawActiveWork |= active > 0;
                }
                catch (Exception ex) when (ex is HttpRequestException or OperationCanceledException)
                {
                    if (sawActiveWork && await database.StringGetAsync("renewal-done") != "yes") failures++;
                }
                if (exit.IsCompleted) break;
                await Task.Delay(500, timeout.Token);
            }
            var logs = await worker.GetLogsAsync(ct: timeout.Token);
            _output.WriteLine(logs.Stdout + logs.Stderr);
            _output.WriteLine($"Workers={workers}; active endpoint timeouts={failures}.");
            Assert.Equal(0, await exit);
            Assert.True(sawActiveWork, "No active jobs were observed.");
            Assert.Equal(0, failures);
        }
    }

    // Only the parent integration test selects this fact in the isolated container.
    public sealed class RenewalWorkerFactAttribute : FactAttribute
    {
        public RenewalWorkerFactAttribute()
        {
            if (Environment.GetEnvironmentVariable("RENEWAL_WORKER_COUNT") == null)
                Skip = "Invoked by the Testcontainers integration test, not in the parent process.";
        }
    }

    public class RedisRenewalWorkerFacts
    {
        [RenewalWorkerFact]
        public void ExecuteJobs()
        {
            var count = int.Parse(Environment.GetEnvironmentVariable("RENEWAL_WORKER_COUNT"));
            var connection = Environment.GetEnvironmentVariable("RENEWAL_REDIS");
            ThreadPool.GetMinThreads(out var minimumWorkers, out var minimumIo);
            Assert.Equal(2, Environment.ProcessorCount);
            Assert.Equal(2, minimumWorkers);
            Assert.Equal(1, minimumIo);
            GlobalConfiguration.Configuration.UseRedisStorage(connection);
            var storage = JobStorage.Current;
            using var mux = ConnectionMultiplexer.Connect(connection);
            RenewalIntegrationJob.Database = mux.GetDatabase();
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseUrls("http://0.0.0.0:9090");
            using var metrics = builder.Build();
            metrics.MapGet("/metrics", () => Volatile.Read(ref RenewalIntegrationJob.Active).ToString());
            metrics.StartAsync().GetAwaiter().GetResult();
            using var server = new BackgroundJobServer(new BackgroundJobServerOptions
            {
                WorkerCount = count, Queues = new[] { "renewal-integration" }, ShutdownTimeout = TimeSpan.FromSeconds(15)
            }, storage);
            var client = new BackgroundJobClient(storage);
            var ids = Enumerable.Range(0, count).Select(key =>
                client.Enqueue(() => RenewalIntegrationJob.ExecuteAsync(key, CancellationToken.None))).ToArray();
            var clock = Stopwatch.StartNew();
            var succeeded = 0;
            var maxActive = 0;
            try
            {
                while (clock.Elapsed < TimeSpan.FromSeconds(100))
                {
                    maxActive = Math.Max(maxActive, Volatile.Read(ref RenewalIntegrationJob.Active));
                    try
                    {
                        using var db = storage.GetConnection();
                        succeeded = ids.Count(id => db.GetStateData(id)?.Name == "Succeeded");
                        if (succeeded == count) break;
                    }
                    catch (RedisTimeoutException) { }
                    Thread.Sleep(200);
                }
                Assert.Equal(count, maxActive);
                Assert.Equal(count, succeeded);
                Assert.Equal(count, RenewalIntegrationJob.Started);
                Assert.Equal(0, RenewalIntegrationJob.Duplicates);
                Assert.Equal(count, Enumerable.Range(0, count).Count(key =>
                    RenewalIntegrationJob.Database.StringGet("renewal-result-" + key) == "saved"));
                ThreadPool.GetMinThreads(out var finalMinimum, out _);
                Assert.Equal(minimumWorkers, finalMinimum);
            }
            finally
            {
                RenewalIntegrationJob.Database.StringSet("renewal-done", "yes");
                server.SendStop();
                server.WaitForShutdown(TimeSpan.FromSeconds(20));
                using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                metrics.StopAsync(shutdown.Token).GetAwaiter().GetResult();
            }
        }
    }

    public static class RenewalIntegrationJob
    {
        internal static IDatabase Database;
        internal static int Active, Started, Duplicates;
        private static readonly ConcurrentDictionary<int, byte> Completed = new ConcurrentDictionary<int, byte>();

        [Queue("renewal-integration"), AutomaticRetry(Attempts = 0), DisableConcurrentExecution("renewal-integration:{0}", 60)]
        public static async Task ExecuteAsync(int key, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Active);
            Interlocked.Increment(ref Started);
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(50), cancellationToken);
                await Database.StringSetAsync("renewal-result-" + key, "saved");
                if (!Completed.TryAdd(key, 1)) Interlocked.Increment(ref Duplicates);
            }
            finally { Interlocked.Decrement(ref Active); }
        }
    }
}
