# Redis renewal availability regression

This opt-in test uses a real Hangfire server, real isolated Redis, real delayed HTTP and a Kestrel health
endpoint. Jobs hold their locks for 50 seconds through the natural 15-second renewal timer. Each worker
container is limited to two CPUs / 1000 MiB and must report default MinWorkers=2. No manual callback,
mocked Redis results, application dependency or raised thread-pool minimum is used.

Requirements: Docker, Python 3.12+ and a .NET 10 SDK image. The image defaults to
`mcr.microsoft.com/dotnet/sdk:10.0`; `--sdk-image` can pin an available digest. This executable is outside
the normal solution build; the provider remains netstandard2.0 and the normal xUnit suite remains net9.0.

```bash
# RED: the same test host and assertions, with the pre-fix provider sources.
python3 tools/renewal-availability.py \
  --source-ref 77698143845b29cbd07414cf54570d9ab4465544 \
  --output artifacts/renewal-red

# GREEN: rebuild the provider from the current working tree.
python3 tools/renewal-availability.py --output artifacts/renewal-green
```

Each output directory must be new. `build-proof.json` records source hashes, SDK image, resolved dependency
and provider DLL checksum; all probes and result/phase assertions are saved alongside it. Only the provider
snapshot and this test host enter the build. The script creates and cleans up its own Redis/worker containers,
network and local HTTP listener, including failure paths. It never contacts or retries production jobs.

The process and Python assertion must both exit 0. At 40 and 80, all jobs must start once, save results,
reach persisted `Succeeded`, achieve full active concurrency and retain default minimum workers. There
must be no timeout of `/metrics` during active work. A zero exit, ready container or successful health request
alone does not pass the test. The observer targets 0.5s probes with a 2s request timeout and 512 KiB body cap;
sampler age and requests after result/shutdown are recorded and classified separately.

Fault controls against owned local keys only:

```bash
REPRO_FAULT=lease-loss python3 tools/renewal-availability.py --output artifacts/lost-leases
REPRO_FAULT=foreign-owner python3 tools/renewal-availability.py --output artifacts/foreign-owners
REPRO_FAULT=pause REPRO_FAULT_AT=14 REPRO_PAUSE_MS=7500 \
  python3 tools/renewal-availability.py --output artifacts/redis-pause
REPRO_SAME_KEY=1 HOLD_SECONDS=20 \
  python3 tools/renewal-availability.py --cases 80 --output artifacts/same-key
```

Loss controls expect all jobs `Failed`, zero saved results, exactly one start and healthy active metrics.
Foreign-owner controls also verify that disposal neither removes nor extends the replacement owner's key.
The same-key control runs 80 jobs in pairs sharing 40 keys and counts any overlap inside the job body.
Normal/fault runs must not be confused: expected failed states are the passing contract of loss tests.

## Red, green, refactor

The branch history separates the regression test from the production fix and later test cleanup.
The baseline is the official merged first fix, not old 1.12.0. The first fix already stops false renewal;
this regression targets remaining synchronous waiting and terminal-state publication on the thread pool.

The earlier application-host experiment found that async renewal alone passes ordinary load but still stalls
under mass loss while synchronous `ChangeState` runs on pool continuations. The second fix keeps renewal
asynchronous and serializes state publication on a dedicated background thread with a bounded queue.
The xUnit suite also checks transient/permanent exceptions, disposal, pending renewals, expected job states
and that a publication exception does not prevent a later notification.

Results of the standalone RED/GREEN verification are recorded below after running the commands. The
provider-only probe is deliberately smaller than the original application host: it is not a full application,
EF/parser workload, production-capacity proof or evidence of the historical OOM cause. Lock cancellation
is cooperative and does not fence arbitrary business side effects. The notification queue is bounded,
best effort and non-durable; saturation/storage errors are logged and do not replay jobs.


### Verified results (2026-10-13 UTC)

| Phase | Workers | Persisted results / Succeeded | Active metrics timeouts | Assertion |
| --- | ---: | ---: | ---: | --- |
| RED, provider `7769814` | 40 | 40 / 40 | 6 | failed (exit 1) |
| RED, provider `7769814` | 80 | 80 / 80 | 19 | failed (exit 1) |
| GREEN, async renewal + isolated publication | 40 | 40 / 40 | 0 | passed (exit 0) |
| GREEN, async renewal + isolated publication | 80 | 80 / 80 | 0 | passed (exit 0) |

RED is a health-availability regression even though business jobs eventually finish: both counts alone
would miss the failure. The first metrics failures were observed while all corresponding owner keys remained valid.
Tests ran on Linux ARM64, 2 CPUs / 1000 MiB, .NET 10.0.300, default MinWorkers=2 / MinIO=1, real Redis 8.4.
The SDK image was pinned locally to build image SHA256
`bd5c728837498c12c5e487a218cbe80f07c73f0db4433c87a90691d558c91946`; it is a local image, not a public image
required by this test. For portable reproduction use the public SDK image argument shown above.

The REFACTOR step extracts a shared job-state assertion from the xUnit theory so the publication-error test
can reuse the behavior without calling another test method. It changes no provider behavior. After refactoring,
the standalone feedback loop and complete xUnit suite are rerun; the verified outcomes are recorded with the branch.

After the REFACTOR step, 40/40 and 80/80 still save results and reach Succeeded with zero active metrics timeouts.
The separate mass-loss control at 40/80 reports all Failed, zero saved results/duplicates, exactly one start per job,
and zero active metrics timeouts. The complete upstream xUnit suite passes **108 / 108, 0 failed, 0 skipped**
against isolated Redis 7. These results were verified on 2026-10-13 and do not imply a production deployment.
