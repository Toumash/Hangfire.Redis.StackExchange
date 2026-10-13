using System.Diagnostics;
using System.Formats.Tar;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

internal static class RenewalAvailabilityRunner
{
    private const string RedisImage = "redis@sha256:43afe4a9a43080e64dfa29699eef903fc9d54991fe8b03d57237c05bed2a396c";
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        WriteIndented = true
    };

    public static async Task<int> RunAsync(string[] args)
    {
        var options = Options.Parse(args);
        var repo = await RunCommandAsync("git", ["rev-parse", "--show-toplevel"]);
        if (Directory.Exists(options.Output) || File.Exists(options.Output))
            throw new IOException("Output must be a new directory; existing evidence is never overwritten.");
        Directory.CreateDirectory(options.Output);
        var app = Path.Combine(options.Output, "app");
        await BuildAsync(repo, options, app);
        var checks = new List<CaseResult>();
        foreach (var count in options.Cases)
        {
            var result = await RunCaseAsync(options, app, count);
            checks.Add(result);
            Console.WriteLine(JsonSerializer.Serialize(new
            {
                result.Workers, result.Passed, activeMetricsFailures = result.ActiveMetricsFailures.Count,
                result.Result, result.ExitCode
            }));
        }
        await SaveAsync(Path.Combine(options.Output, "assertions.json"), checks);
        return checks.All(result => result.Passed) ? 0 : 1;
    }

    public static async Task RunProviderAsync()
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://0.0.0.0:9091");
        await using var app = builder.Build();
        var delay = TimeSpan.FromSeconds(double.Parse(Environment.GetEnvironmentVariable("HOLD_SECONDS") ?? "50",
            CultureInfo.InvariantCulture));
        app.MapGet("/health", () => "ok");
        app.MapGet("/wait", async (CancellationToken ct) =>
        {
            await Task.Delay(delay, ct);
            return new { ok = true };
        });
        await app.RunAsync();
    }

    private static async Task BuildAsync(string repo, Options options, string app)
    {
        var scratch = Path.Combine(Path.GetTempPath(), "renewal-build-" + Guid.NewGuid().ToString("N"));
        var buildContainer = "renewal-build-" + Guid.NewGuid().ToString("N");
        Directory.CreateDirectory(scratch);
        try
        {
            var library = Path.Combine(scratch, "library");
            var probe = Path.Combine(scratch, "probe");
            var sourceRef = "working-tree";
            if (options.SourceRef is not null)
            {
                sourceRef = await RunCommandAsync("git", ["-C", repo, "rev-parse", "--verify", options.SourceRef + "^{commit}"]);
                var archive = Path.Combine(scratch, "source.tar");
                await RunCommandAsync("git", ["-C", repo, "archive", "--output=" + archive, sourceRef]);
                Directory.CreateDirectory(library);
                TarFile.ExtractToDirectory(archive, library, overwriteFiles: false);
            }
            else
            {
                CopySource(Path.Combine(repo, "Hangfire.Redis.StackExchange"), Path.Combine(library, "Hangfire.Redis.StackExchange"));
                File.Copy(Path.Combine(repo, "README.md"), Path.Combine(library, "README.md"));
            }
            CopySource(Path.Combine(repo, "tools", "RenewalAvailabilityProbe"), probe);
            var sourceHashes = Directory.GetFiles(Path.Combine(library, "Hangfire.Redis.StackExchange"),
                "*.cs", SearchOption.AllDirectories)
                .ToDictionary(path => Path.GetRelativePath(library, path), HashFile);
            await RunCommandAsync("docker", ["run", "--rm", "--name", buildContainer,
                "-v", library + ":/library", "-v", probe + ":/probe",
                "-v", options.Output + ":/out", "-w", "/probe", options.SdkImage, "dotnet", "publish",
                "RenewalAvailabilityProbe.csproj", "-c", "Release", "-o", "/out/app",
                "-p:RedisProjectPath=/library/Hangfire.Redis.StackExchange/Hangfire.Redis.StackExchange.csproj"],
                TimeSpan.FromMinutes(5), Path.Combine(options.Output, "build.log"));
            using var deps = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(app, "RenewalAvailabilityProbe.deps.json")));
            var libraries = deps.RootElement.GetProperty("libraries").EnumerateObject().Select(x => x.Name).ToArray();
            await SaveAsync(Path.Combine(options.Output, "build-proof.json"), new
            {
                sourceRef, sourceHashes, sdkImage = options.SdkImage,
                dllSha256 = HashFile(Path.Combine(app, "Hangfire.Redis.StackExchange.dll")),
                library = libraries.Single(x => x.StartsWith("Hangfire.Redis.StackExchange/", StringComparison.Ordinal))
            });
        }
        finally
        {
            await IgnoreFailureAsync("docker", ["rm", "-f", "-v", buildContainer]);
            try { Directory.Delete(scratch, recursive: true); }
            catch (UnauthorizedAccessException)
            {
                // Native Linux bind mounts may contain root-owned build outputs. Only remove this owned snapshot.
                await RunCommandAsync("docker", ["run", "--rm", "-v", scratch + ":/scratch", options.SdkImage,
                    "rm", "-rf", "/scratch/library", "/scratch/probe", "/scratch/source.tar"]);
                Directory.Delete(scratch, recursive: true);
            }
        }
    }

    private static async Task<CaseResult> RunCaseAsync(Options options, string app, int count)
    {
        var name = "renewal-availability-" + Guid.NewGuid().ToString("N")[..8];
        var redisName = name + "-redis";
        var workerName = name + "-worker";
        var providerName = name + "-provider";
        var directory = Path.Combine(options.Output, "case-" + count);
        Directory.CreateDirectory(directory);
        Process? logs = null;
        Task? logCopy = null;
        var rows = new List<Observation>();
        Fault? fault = null;
        try
        {
            await DockerAsync("network", "create", name);
            await DockerAsync("run", "-d", "--name", redisName, "--network", name, "--network-alias", "redis",
                "-p", "127.0.0.1::6379", RedisImage);
            var redisPort = await PortAsync(redisName, 6379);
            using var mux = await ConnectionMultiplexer.ConnectAsync(
                $"127.0.0.1:{redisPort},asyncTimeout=2000,connectTimeout=2000,connectRetry=0,abortConnect=false");
            var database = mux.GetDatabase();
            await WaitRedisAsync(database);
            await DockerAsync("run", "-d", "--name", providerName, "--network", name, "--network-alias", "provider",
                "--cpus=1", "--memory=256m", "-e", "HOLD_SECONDS=" + options.HoldSeconds.ToString(CultureInfo.InvariantCulture),
                "-v", app + ":/app:ro", "-w", "/app", "-p", "127.0.0.1::9091", options.SdkImage,
                "dotnet", "/app/RenewalAvailabilityProbe.dll", "--provider");
            var providerPort = await PortAsync(providerName, 9091);
            using var http = new HttpClient(new HttpClientHandler { UseProxy = false });
            await WaitHealthyAsync(http, $"http://127.0.0.1:{providerPort}/health");
            await DockerAsync("run", "-d", "--name", workerName, "--network", name, "--cpus=2", "--memory=1000m",
                "-e", "REPRO_REDIS=redis:6379", "-e", "REPRO_PROVIDER=http://provider:9091/wait",
                "-e", "REPRO_EXPECT_LOSS=" + (options.ExpectsLoss ? "1" : "0"), "-e", "REPRO_KEYS=/out/keys.jsonl",
                "-e", "REPRO_SAME_KEY=" + (options.SameKey ? "1" : "0"), "-v", app + ":/app:ro",
                "-v", directory + ":/out", "-w", "/app", "-p", "127.0.0.1::9090", options.SdkImage,
                "dotnet", "/app/RenewalAvailabilityProbe.dll", count.ToString());
            var port = await PortAsync(workerName, 9090);
            logs = StartCommand("docker", ["logs", "-f", workerName]);
            logCopy = CopyLogAsync(logs, directory);
            var wait = DockerAsync("wait", workerName);
            var clock = Stopwatch.StartNew();
            var seenHealthy = false;
            while (clock.Elapsed < TimeSpan.FromSeconds(145) && !wait.IsCompleted)
            {
                var began = Stopwatch.StartNew();
                var row = new Observation { Utc = DateTimeOffset.UtcNow, ElapsedMs = clock.ElapsedMilliseconds };
                try
                {
                    await ReadHealthAsync(http, $"http://127.0.0.1:{port}/metrics");
                    row.Healthy = true;
                    seenHealthy = true;
                }
                catch (Exception ex) when (ex is HttpRequestException or OperationCanceledException)
                {
                    row.ErrorType = ex.GetType().Name;
                }
                row.RequestMs = began.ElapsedMilliseconds;
                row.HealthyPreviouslyObserved = seenHealthy;
                try
                {
                    var keys = ReadKeys(directory);
                    if (options.Fault != "none" && fault is null && clock.Elapsed.TotalSeconds > options.FaultAt + 10)
                        throw new InvalidOperationException("Fault precondition was not reached within the injection window.");
                    var values = keys.Count == 0 ? [] : await database.StringGetAsync(keys.Keys.Select(x => (RedisKey)x).ToArray());
                    row.Keys = keys.Count;
                    row.MatchingOwners = values.Zip(keys.Values).Count(x => !x.First.IsNull && Hash(x.First.ToString()) == x.Second);
                    row.MissingOwners = values.Count(x => x.IsNull);
                    if (options.Fault != "none" && fault is null && clock.Elapsed.TotalSeconds >= options.FaultAt && keys.Count == count)
                    {
                        await ApplyFaultAsync(database, options, keys.Keys.ToArray());
                        fault = new Fault(options.Fault, DateTimeOffset.UtcNow, keys.Count, clock.ElapsedMilliseconds);
                    }
                }
                catch (RedisException ex)
                {
                    row.OwnerReadErrorType = ex.GetType().Name;
                }
                rows.Add(row);
                await SaveAsync(Path.Combine(directory, "metrics-observer.json"), rows);
                var remaining = TimeSpan.FromMilliseconds(500) - began.Elapsed;
                if (remaining > TimeSpan.Zero) await Task.Delay(remaining);
            }
            var forced = !wait.IsCompleted;
            if (forced) await DockerAsync("kill", workerName);
            var exitCode = int.Parse(await wait);
            await logCopy.WaitAsync(TimeSpan.FromSeconds(10));
            var events = ReadEvents(directory);
            var samples = events.Where(x => x.Kind == "sample").ToArray();
            var startup = events.FirstOrDefault(x => x.Kind == "startup");
            var result = events.FirstOrDefault(x => x.Kind == "result");
            var activeFailures = new List<Observation>();
            var activeSuccesses = 0;
            foreach (var row in rows)
            {
                if (result is not null && row.Utc >= result.Utc) continue;
                var sample = samples.LastOrDefault(x => x.Utc <= row.Utc);
                if (sample is null || sample.Active == 0) continue;
                if (row.Healthy) activeSuccesses++;
                else if (row.HealthyPreviouslyObserved)
                {
                    row.SampleAgeSeconds = (row.Utc - sample.Utc).TotalSeconds;
                    activeFailures.Add(row);
                }
            }
            ForeignOwners? foreign = null;
            if (options.Fault == "foreign-owner" && fault is not null)
            {
                var keys = ReadKeys(directory).Keys.Select(x => (RedisKey)x).ToArray();
                var values = await database.StringGetAsync(keys);
                var ttl = await Task.WhenAll(keys.Select(async key => (await database.KeyTimeToLiveAsync(key))?.TotalMilliseconds ?? -1));
                foreign = new ForeignOwners(values.Count(x => x == "foreign-owner"), ttl.Min(), ttl.Max());
            }
            var expectedResults = result is not null && (options.ExpectsLoss
                ? result.States?.GetValueOrDefault("Failed") == count && result.Saved == 0 && result.Completed == 0
                : result.Succeeded == count && result.Saved == count);
            var maxActive = samples.Select(x => x.Active).DefaultIfEmpty().Max();
            var passed = exitCode == 0 && !forced && expectedResults && result!.Started == count && result.Duplicates == 0 &&
                result.Overlaps == 0 && result.MinimumWorkersUnchanged && startup?.MinimumWorkers == 2 &&
                startup.MinimumIo == 1 && startup.ProcessorCount == 2 &&
                maxActive == (options.SameKey ? count / 2 : count) && activeFailures.Count == 0 && activeSuccesses >= 40 &&
                (options.Fault == "none" || fault is not null) &&
                (options.Fault != "foreign-owner" || foreign is { Count: var owners, MinTtlMs: > 0, MaxTtlMs: < 55000 } && owners == count);
            var check = new CaseResult(count, passed, exitCode, result, startup, activeSuccesses, activeFailures, maxActive,
                samples.Select(x => x.Pending).DefaultIfEmpty().Max(), samples.Select(x => x.WorkingSet).DefaultIfEmpty().Max(),
                rows.Count(x => !x.Healthy && x.HealthyPreviouslyObserved) - activeFailures.Count, fault, foreign, forced,
                DateTimeOffset.UtcNow,
                "0.5s target point probes; stale sample age retained; not continuous availability or application E2E");
            await SaveAsync(Path.Combine(directory, "summary.json"), check);
            return check;
        }
        finally
        {
            foreach (var container in new[] { workerName, providerName, redisName })
                await IgnoreFailureAsync("docker", ["rm", "-f", "-v", container]);
            await IgnoreFailureAsync("docker", ["network", "rm", name]);
            if (logs is { HasExited: false }) logs.Kill(entireProcessTree: true);
            if (logCopy is not null) await logCopy.WaitAsync(TimeSpan.FromSeconds(10));
            logs?.Dispose();
        }
    }

    private static async Task ApplyFaultAsync(IDatabase database, Options options, string[] keys)
    {
        switch (options.Fault)
        {
            case "lease-loss": await database.KeyDeleteAsync(keys.Select(x => (RedisKey)x).ToArray()); break;
            case "foreign-owner":
                await Task.WhenAll(keys.Select(key => database.StringSetAsync(key, "foreign-owner", TimeSpan.FromSeconds(60))));
                break;
            case "pause": await database.ExecuteAsync("CLIENT", "PAUSE", options.PauseMs, "ALL"); break;
        }
    }

    private static Dictionary<string, string> ReadKeys(string directory)
    {
        var keys = new Dictionary<string, string>();
        var path = Path.Combine(directory, "keys.jsonl");
        if (!File.Exists(path)) return keys;
        foreach (var line in File.ReadLines(path))
        {
            try
            {
                using var json = JsonDocument.Parse(line);
                keys[json.RootElement.GetProperty("key").GetString()!] = json.RootElement.GetProperty("ownerSha256").GetString()!;
            }
            catch (JsonException) { } // Ignore only an incomplete final line while the child writes it.
        }
        return keys;
    }

    private static List<ProbeEvent> ReadEvents(string directory) => File.ReadLines(Path.Combine(directory, "host.log"))
        .Select(line =>
        {
            try { return JsonSerializer.Deserialize<ProbeEvent>(line, JsonOptions); }
            catch (JsonException) { return null; }
        }).OfType<ProbeEvent>().ToList();

    private static async Task ReadHealthAsync(HttpClient http, string url)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        using var response = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, timeout.Token);
        response.EnsureSuccessStatusCode();
        await response.Content.LoadIntoBufferAsync(512_000, timeout.Token);
    }

    private static async Task WaitHealthyAsync(HttpClient http, string url)
    {
        for (var i = 0; i < 30; i++)
        {
            try { await ReadHealthAsync(http, url); return; }
            catch (Exception ex) when (ex is HttpRequestException or OperationCanceledException) { await Task.Delay(200); }
        }
        throw new TimeoutException("Owned delayed HTTP provider did not start.");
    }

    private static async Task WaitRedisAsync(IDatabase database)
    {
        var deadline = Stopwatch.StartNew();
        while (deadline.Elapsed < TimeSpan.FromSeconds(15))
        {
            try { await database.PingAsync(); return; }
            catch (RedisException) { await Task.Delay(200); }
        }
        throw new TimeoutException("Owned Redis did not start.");
    }

    private static async Task<int> PortAsync(string container, int port)
    {
        using var json = JsonDocument.Parse(await DockerAsync("inspect", container));
        return int.Parse(json.RootElement[0].GetProperty("NetworkSettings").GetProperty("Ports")
            .GetProperty(port + "/tcp")[0].GetProperty("HostPort").GetString()!);
    }

    private static Task<string> DockerAsync(params string[] args) => RunCommandAsync("docker", args, TimeSpan.FromMinutes(3));

    private static Process StartCommand(string file, IEnumerable<string> args)
    {
        var info = new ProcessStartInfo(file) { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        foreach (var argument in args) info.ArgumentList.Add(argument);
        return Process.Start(info) ?? throw new IOException("Could not start " + file);
    }

    private static async Task<string> RunCommandAsync(string file, string[] args, TimeSpan? timeout = null, string? log = null)
    {
        using var process = StartCommand(file, args);
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        try { await process.WaitForExitAsync().WaitAsync(timeout ?? TimeSpan.FromSeconds(30)); }
        catch { process.Kill(entireProcessTree: true); throw; }
        var output = await stdout;
        var errors = await stderr;
        if (log is not null) await File.WriteAllTextAsync(log, output + errors);
        if (process.ExitCode != 0) throw new IOException($"{file} exited {process.ExitCode}: {errors}");
        return output.Trim();
    }

    private static async Task CopyLogAsync(Process process, string directory)
    {
        await using var stdout = File.Create(Path.Combine(directory, "host.log"));
        await using var stderr = File.Create(Path.Combine(directory, "host.stderr.log"));
        await Task.WhenAll(process.StandardOutput.BaseStream.CopyToAsync(stdout), process.StandardError.BaseStream.CopyToAsync(stderr));
    }

    private static async Task IgnoreFailureAsync(string command, string[] args)
    {
        try { await RunCommandAsync(command, args); }
        catch (IOException ex) when (ex.Message.Contains("No such container", StringComparison.OrdinalIgnoreCase)) { }
        catch (Exception ex) when (ex is IOException or TimeoutException)
        {
            Console.Error.WriteLine($"Owned resource cleanup failed: {ex.GetType().Name}");
        }
    }

    private static void CopySource(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (var file in Directory.GetFiles(source)) File.Copy(file, Path.Combine(destination, Path.GetFileName(file)));
        foreach (var child in Directory.GetDirectories(source))
            if (Path.GetFileName(child) is not ("bin" or "obj")) CopySource(child, Path.Combine(destination, Path.GetFileName(child)));
    }

    private static string HashFile(string path) => Convert.ToHexStringLower(SHA256.HashData(File.ReadAllBytes(path)));
    private static string Hash(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
    private static Task SaveAsync<T>(string path, T value) =>
        File.WriteAllTextAsync(path, JsonSerializer.Serialize(value, JsonOptions) + "\n");

    private sealed record Options(string Output, string? SourceRef, string SdkImage, int[] Cases,
        string Fault, double FaultAt, int PauseMs, double HoldSeconds, bool SameKey)
    {
        public bool ExpectsLoss => Fault is "lease-loss" or "foreign-owner";
        public static Options Parse(string[] args)
        {
            var values = new Dictionary<string, string>();
            for (var i = 0; i < args.Length; i += 2)
            {
                if (i + 1 >= args.Length || args[i] is not ("--output" or "--source-ref" or "--sdk-image" or "--cases"))
                    throw new ArgumentException("Usage: --output NEW_DIRECTORY [--source-ref COMMIT] [--sdk-image IMAGE] [--cases 40,80]");
                values.Add(args[i], args[i + 1]);
            }
            var cases = values.GetValueOrDefault("--cases", "40,80").Split(',').Select(int.Parse).ToArray();
            var fault = Environment.GetEnvironmentVariable("REPRO_FAULT") ?? "none";
            if (!values.ContainsKey("--output") || cases.Length == 0 || cases.Distinct().Count() != cases.Length ||
                cases.Any(x => x is not (40 or 80)) ||
                fault is not ("none" or "lease-loss" or "foreign-owner" or "pause")) throw new ArgumentException("Invalid cases/fault.");
            return new Options(Path.GetFullPath(values["--output"]), values.GetValueOrDefault("--source-ref"),
                values.GetValueOrDefault("--sdk-image", "mcr.microsoft.com/dotnet/sdk:10.0"), cases, fault,
                double.Parse(Environment.GetEnvironmentVariable("REPRO_FAULT_AT") ?? "20", CultureInfo.InvariantCulture),
                int.Parse(Environment.GetEnvironmentVariable("REPRO_PAUSE_MS") ?? "3500"),
                double.Parse(Environment.GetEnvironmentVariable("HOLD_SECONDS") ?? "50", CultureInfo.InvariantCulture),
                Environment.GetEnvironmentVariable("REPRO_SAME_KEY") == "1");
        }
    }

    private sealed class Observation
    {
        public DateTimeOffset Utc { get; init; }
        public long ElapsedMs { get; init; }
        public bool Healthy { get; set; }
        public string? ErrorType { get; set; }
        public long RequestMs { get; set; }
        public bool HealthyPreviouslyObserved { get; set; }
        public int Keys { get; set; }
        public int MatchingOwners { get; set; }
        public int MissingOwners { get; set; }
        public string? OwnerReadErrorType { get; set; }
        public double? SampleAgeSeconds { get; set; }
    }

    private sealed record ProbeEvent(string Kind, DateTimeOffset Utc, int Active, long Pending, long WorkingSet,
        int Started, int Completed, int Succeeded, int Saved, Dictionary<string, int>? States,
        int Duplicates, int Overlaps, bool MinimumWorkersUnchanged, int MinimumWorkers, int MinimumIo,
        int ProcessorCount, Guid LibraryMvid);
    private sealed record Fault(string Name, DateTimeOffset Utc, int Keys, long ElapsedMs);
    private sealed record ForeignOwners(int Count, double MinTtlMs, double MaxTtlMs);
    private sealed record CaseResult(int Workers, bool Passed, int ExitCode, ProbeEvent? Result, ProbeEvent? Startup,
        int ActiveMetricsSuccesses, List<Observation> ActiveMetricsFailures, int MaxActive, long MaxPending,
        long MaxWorkingSet, int ShutdownOrInactiveMetricsFailures, Fault? Fault, ForeignOwners? ForeignOwners,
        bool ForcedLocalCleanup, DateTimeOffset CompletedUtc, string SamplingLimit);
}
