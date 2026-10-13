# Redis lock renewal integration tests

Run from the repository root with Docker available:

```bash
dotnet test Hangfire.Redis.Tests/Hangfire.Redis.Tests.csproj -c Release \
  --filter FullyQualifiedName~RedisRenewalIntegrationFacts
```

Testcontainers starts an isolated Redis 7.4 container and a Linux .NET 9 SDK container running the selected
xUnit worker fact. The test copies compiled test binaries; it does not build a custom image, run shell scripts,
change production settings or require a separate example executable. Resources are disposed after each case
and remain covered by the resource reaper. `RENEWAL_TEST_SDK_IMAGE` optionally selects another SDK image.

The worker has 2 CPUs / 1000 MiB and checks the default minimum worker/IO thread counts (2/1). Actual Hangfire
jobs hold unique distributed locks across a 50-second asynchronous wait, spanning several natural 15-second
renewals. The parent probes the worker's Kestrel endpoint independently. Assertions require full 40/80 active
concurrency, exactly one execution/result per job, persisted Succeeded states, unchanged minimum workers and
no active endpoint timeout. The parent-host worker fact is intentionally skipped; the parent integration tests
execute it inside their container, preventing global thread-pool changes in the main test process.

The baseline is upstream merge `77698143845b29cbd07414cf54570d9ab4465544`, which already contains the first
false-renewal fix. With the same new tests on that baseline, the 80-job worker aborted with an unhandled
BackgroundJobClientException/RedisTimeoutException in RedisLock.ExpirationTimerTick (RED); the 40-job case
passed in that run, so this remains sensitive to scheduling rather than a universal worker-count threshold.
With asynchronous renewal and isolated failure publication, both integration cases pass (GREEN).
Full suite verification: 110 passed / 0 failed / 1 intentional parent-host worker-fact skip on 2026-10-13.

This is a provider integration test with synthetic asynchronous jobs, not a full application HTTP/EF/parser
workload, production-capacity measurement or proof of the historical OOM cause. Existing targeted tests cover
lease loss, disposal races, transient/permanent exceptions, expected job states and publication failures.
The notification queue remains bounded, best effort and non-durable; it is not business-side-effect fencing.
