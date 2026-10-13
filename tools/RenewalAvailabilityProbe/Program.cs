// Test-only host: real Hangfire/Redis, natural timers and the default thread pool.
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Hangfire;
using Hangfire.Common;
using Hangfire.Redis.StackExchange;
using Hangfire.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using StackExchange.Redis;

var count = int.Parse(args[0]);
var connection = Environment.GetEnvironmentVariable("REPRO_REDIS") ?? "redis:6379";
ProbeJob.SameKey = Environment.GetEnvironmentVariable("REPRO_SAME_KEY") == "1";
ProbeJob.Provider = Environment.GetEnvironmentVariable("REPRO_PROVIDER")!;
ThreadPool.GetMinThreads(out var minimumWorkers, out var minimumIo);
Console.WriteLine(JsonSerializer.Serialize(new { kind = "startup", utc = DateTimeOffset.UtcNow,
    workers = count, processorCount = Environment.ProcessorCount, minimumWorkers, minimumIo,
    libraryMvid = typeof(RedisStorage).Assembly.ManifestModule.ModuleVersionId,
    scope = "Public Hangfire server + real Redis + delayed HTTP + Kestrel health endpoint; isolated library regression, not application E2E" }));
var prefix = $"{{renewal-probe-{count}}}:";
GlobalConfiguration.Configuration.UseRedisStorage(connection, new RedisStorageOptions { Prefix = prefix });
var storage = JobStorage.Current;
using var mux = ConnectionMultiplexer.Connect(connection);
ProbeJob.Database = mux.GetDatabase();
GlobalJobFilters.Filters.Add(new InspectLockFilter { Order = 1 });
var builder = WebApplication.CreateSlimBuilder();
builder.Logging.ClearProviders();
builder.WebHost.UseUrls("http://0.0.0.0:9090");
using var metrics = builder.Build();
metrics.MapGet("/metrics", () => $"thread_pool_pending {ThreadPool.PendingWorkItemCount}\n");
metrics.StartAsync().GetAwaiter().GetResult();
using var server = new BackgroundJobServer(new BackgroundJobServerOptions
{
    WorkerCount = count, Queues = new[] { "diagnostic" }, ShutdownTimeout = TimeSpan.FromSeconds(15)
}, storage);
var client = new BackgroundJobClient(storage);
var ids = new List<string>();
var clock = Stopwatch.StartNew();
var done = false;
var sampler = new Thread(() => {
    while (!Volatile.Read(ref done)) {
        using var process = Process.GetCurrentProcess();
        Console.WriteLine(JsonSerializer.Serialize(new { kind = "sample", utc = DateTimeOffset.UtcNow,
            elapsedMs = clock.ElapsedMilliseconds, active = ProbeJob.Active, started = ProbeJob.Started,
            completed = ProbeJob.Completed.Count, poolThreads = ThreadPool.ThreadCount,
            pending = ThreadPool.PendingWorkItemCount, completedPool = ThreadPool.CompletedWorkItemCount,
            workingSet = process.WorkingSet64, lockedKeys = InspectLockFilter.Keys.Count }));
        Thread.Sleep(500);
    }
}) { IsBackground = true, Name = "independent-repro-sampler" };
sampler.Start();
for (var i = 0; i < count; i++) { var key = i; var group = ProbeJob.SameKey ? i / 2 : i; ids.Add(client.Enqueue<ProbeJob>(j => j.ExecuteAsync(group, key, CancellationToken.None))); }
var pollErrors = 0;
var succeeded = 0;
Dictionary<string,int> states = new();
var deadline = Stopwatch.StartNew();
while (deadline.Elapsed < TimeSpan.FromSeconds(95)) {
    try {
        using var c = storage.GetConnection();
        states = ids.Select(id => c.GetStateData(id)?.Name ?? "Missing").GroupBy(x => x)
            .ToDictionary(g => g.Key, g => g.Count());
        succeeded = states.GetValueOrDefault("Succeeded");
        if (succeeded == count || (states.GetValueOrDefault("Failed") == count && ProbeJob.Active == 0)) break;
    } catch (RedisTimeoutException) { pollErrors++; }
    Thread.Sleep(1000);
}
var saved = 0;
try { saved = Enumerable.Range(0, count).Count(i => ProbeJob.Database.StringGet($"result-{i}") == "saved"); }
catch (RedisTimeoutException) { pollErrors++; }
Console.WriteLine(JsonSerializer.Serialize(new { kind = "result", utc = DateTimeOffset.UtcNow,
    workers = count, started = ProbeJob.Started, completed = ProbeJob.Completed.Count,
    succeeded, saved, states, pollErrors, duplicates = ProbeJob.Duplicates, overlaps = ProbeJob.Overlaps,
    minimumWorkersUnchanged = GetMinimum() == minimumWorkers }));
try { server.SendStop(); server.WaitForShutdown(TimeSpan.FromSeconds(20)); metrics.StopAsync(TimeSpan.FromSeconds(5)).GetAwaiter().GetResult(); }
catch (Exception e) { Console.WriteLine(JsonSerializer.Serialize(new { kind = "shutdown", errorType = e.GetType().Name })); }
done = true;
sampler.Join(TimeSpan.FromSeconds(2));
var expectedLoss = Environment.GetEnvironmentVariable("REPRO_EXPECT_LOSS") == "1";
var expectedResults = expectedLoss
    ? states.GetValueOrDefault("Failed") == count && saved == 0 && ProbeJob.Completed.Count == 0
    : succeeded == count && saved == count;
Environment.ExitCode = expectedResults && ProbeJob.Started == count && ProbeJob.Duplicates == 0 && ProbeJob.Overlaps == 0 ? 0 : 1;
static int GetMinimum() { ThreadPool.GetMinThreads(out var value, out _); return value; }

public sealed class ProbeJob {
    private static readonly HttpClient Http = new() { Timeout = TimeSpan.FromSeconds(80) };
    public static string Provider = "";
    public static IDatabase Database = null!;
    public static ConcurrentDictionary<int, byte> Completed = new();
    public static int Active, Started, Duplicates, Overlaps;
    public static bool SameKey;
    private static readonly ConcurrentDictionary<int, int> InGroup = new();
    [Queue("diagnostic")]
    [AutomaticRetry(Attempts = 0)]
    [DisableConcurrentExecution("diagnostic Natural:{0}", 60)]
    public async Task ExecuteAsync(int group, int key, CancellationToken ct) {
        Interlocked.Increment(ref Started); Interlocked.Increment(ref Active);
        if (InGroup.AddOrUpdate(group, 1, (_, n) => n + 1) > 1) Interlocked.Increment(ref Overlaps);
        try {
            await Http.GetStringAsync(Provider, ct);
            await Database.StringSetAsync($"result-{key}", "saved");
            if (!Completed.TryAdd(key, 1)) Interlocked.Increment(ref Duplicates);
        } finally { InGroup.AddOrUpdate(group, 0, (_, n) => n - 1); Interlocked.Decrement(ref Active); }
    }
}
public sealed class InspectLockFilter : JobFilterAttribute, IServerFilter {
    public static ConcurrentDictionary<int, string> Keys = new();
    public void OnPerforming(PerformingContext context) {
        if (context.BackgroundJob.Job.Type != typeof(ProbeJob)) return;
        var lease = context.Items.Values.Single(v => v?.GetType().FullName == "Hangfire.Redis.StackExchange.RedisLock")!;
        var type = lease.GetType();
        var key = type.GetField("_key", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(lease)!.ToString()!;
        var owner = type.GetField("OwnerId", BindingFlags.Static | BindingFlags.NonPublic)!.GetValue(null)!.ToString()!;
        Keys[(int)context.BackgroundJob.Job.Args[1]] = key;
        var output = Environment.GetEnvironmentVariable("REPRO_KEYS") ?? "/probe/keys.jsonl";
        lock (Keys) File.AppendAllText(output, JsonSerializer.Serialize(new { key,
            ownerSha256 = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(owner))) }) + "\n");
        // Inspect only: no manual timer invocation, timer changes, proxy, or key mutation.
    }
    public void OnPerformed(PerformedContext context) { }
}
