# Kelpie Completion Acknowledgement Failure Design

## Status

- Proposed
- Scope: Kelpie worker only
- Production incident: GLP1R docking, August 5, 2026
- Related worker commits: `5888655`, `35aa24c`, `eb62df4`

## Summary

A successful job command and a successful acknowledgement to the Kelpie API are
different facts. The worker currently allows those facts to collapse into one
error path: after a command exits zero, a failed `POST /completed` can reach a
catch block that calls `POST /failed`.

That behavior is semantically incorrect. A control-plane transport failure
does not change a successful workload into a failed workload. It also creates
expensive retry and cancellation behavior because the queue is told to retry
work whose durable outputs already exist.

This design introduces one small finalization boundary for successful jobs:

```text
payload and required uploads succeed
  -> job becomes completion-eligible
  -> stop the job heartbeat
  -> attempt /completed with existing finite retries
  -> acknowledged: record completed
  -> exhausted: record completion_pending, never call /failed
```

No new queue, service, environment variable, wire format, or external
reconciler is introduced. If acknowledgement remains pending, Kelpie's existing
stale-heartbeat reassignment recovers the job. Docking's R2 `_SUCCESS` check
makes that reassigned attempt a cheap no-op before it retries `/completed`.

## Incident Motivation

The confirmed incident sequence was:

1. `screen_shard.py` completed successfully.
2. Scores, poses, summaries, and `_SUCCESS` were uploaded to R2.
3. The child process exited with code zero.
4. `POST /jobs/{id}/completed` exhausted three retries during Cloudflare D1
   overload.
5. The thrown completion exception reached a generic worker catch block.
6. The worker called `POST /jobs/{id}/failed` and logged
   `exit_code=0, exit_action=failed`.
7. A concurrent heartbeat failure then restarted the old heartbeat loop. The
   heartbeat restart has since been fixed separately.

The reliability audit found `1,985` `exit_code=0, exit_action=failed` events in
the seven-day replay window. Those events represented `464.72` worker-hours of
successful process runtime placed into the wrong terminal path. This design
addresses the semantic failure regardless of whether the completion request
failed because of D1 overload, a timeout, authentication failure, a lost
response, or another transport exception.

## Systems and Responsibilities

### Job command

The command determines the workload outcome:

- Exit zero means the payload succeeded.
- Exit nonzero means the payload failed unless remote cancellation owns the
  outcome.
- The command is responsible for application-specific durable output. For the
  docking pipeline, `_SUCCESS` is written to R2 last.

### Kelpie worker

The worker owns one local job attempt:

- Start and supervise the command process group.
- Keep the job lease alive while the command and required final uploads run.
- Stop heartbeat when successful local work is durable.
- Report the immutable workload outcome to the Kelpie API.
- Never reinterpret a completion transport error as workload failure.

### Kelpie API

The API owns remote queue state:

- `running` identifies the current lease.
- A successful `/completed` request transitions the job to `completed`.
- A stale heartbeat causes an interrupted job to be reassigned.
- Interruption does not increment `num_failures`.
- `/failed` is reserved for actual workload or required-upload failure.

### Docking tranche and R2

The tranche manifest maps shards to Kelpie jobs. R2 output is keyed by stable
run/shard identity rather than Kelpie attempt identity. The `_SUCCESS` marker
is the docking application's durable evidence that a shard completed.

The current startup behavior is already idempotent:

```text
reassigned job starts
  -> screen_shard.py checks run/shard/_SUCCESS
  -> marker exists
  -> command exits zero without docking
  -> worker retries /completed
```

The tranche service does not call the worker-only `/completed` endpoint in this
design. That endpoint has no documented administrative reconciliation contract.

## Terminology

**Payload success**
: The command exited zero and was not remotely canceled.

**Required final uploads**
: Uploads Kelpie must finish before the job can be considered successful,
including `sync.after` and legacy output-directory uploads.

**Completion-eligible**
: The payload succeeded and every required final upload completed. From this
point onward, `/failed` is forbidden for the attempt.

**Completion acknowledgement**
: The successful response from `POST /jobs/{id}/completed`.

**Completion pending**
: The attempt is completion-eligible, but the worker exhausted its finite
completion-report retries without observing acknowledgement.

Completion pending is a worker telemetry outcome. It is not a new Kelpie API
job status.

## Invariants

1. Payload outcome and control-plane acknowledgement are independent.
2. Only nonzero payload exit or required-upload failure may call `/failed`.
3. Once completion-eligible, no later exception may call `/failed`.
4. Completion acknowledgement failure must not increment `num_failures`.
5. Heartbeat runs while the payload and required final uploads are active.
6. Heartbeat stops before the completion acknowledgement request.
7. A completion acknowledgement exception cannot escape into a generic failure
   handler.
8. Post-eligibility cleanup is best-effort and cannot change job outcome.
9. Every job attempt owns at most one heartbeat loop, and stopped heartbeat is
   terminal.
10. Recovery may execute more than once, so completion reporting and docking
    startup must remain idempotent.

## Current Failure Paths

There are three successful-job paths in `src/index.ts`, and they do not
currently have one consistent acknowledgement boundary.

### No final sync

The worker stops heartbeat and calls `reportCompleted()`. If that call throws,
the exception reaches the outer catch, which sets `observedExitAction` to
`failed` and calls `reportFailed()`.

This is the exact path from the August 5 incident.

### `sync.after`

Upload and completion are currently joined in one promise chain:

```text
Promise.all(uploads)
  -> stop heartbeat
  -> reportCompleted
  -> shared catch calls reportFailed
```

The shared catch cannot distinguish an upload failure from an acknowledgement
failure. Upload failure is a legitimate job failure; acknowledgement failure
is not.

### Legacy output upload

This path already catches a failed completion report without calling
`reportFailed()`. It nevertheless has separate logic, does not surface a
distinct `completion_pending` action, and performs cleanup outside a shared
post-eligibility boundary.

### Post-completion cleanup

Filesystem cleanup can still throw after required output has been uploaded or
after `/completed` succeeded. If that exception reaches the generic outer
catch, the worker can incorrectly call `/failed` after completion eligibility.

The fix must therefore separate all post-eligibility work from the generic
failure path, not merely add one special-case catch around `/completed`.

## Chosen Design

### One successful-finalization seam

Add a small `src/completion.ts` module. It owns the transition from successful
local work to acknowledged or pending completion.

```typescript
export enum CompletionAckStatus {
  Acknowledged = "acknowledged",
  Pending = "pending",
}

export type CompletionAckResult =
  | {
      status: CompletionAckStatus.Acknowledged;
      heartbeatStopError?: string;
      cleanupError?: string;
    }
  | {
      status: CompletionAckStatus.Pending;
      completionReportError: string;
      heartbeatStopError?: string;
      cleanupError?: string;
    };

export type FinalizeSuccessfulJobOptions = {
  uploadFinalArtifacts?: () => Promise<void>;
  stopHeartbeat: () => Promise<void>;
  reportCompletion: () => Promise<void>;
  cleanup?: () => Promise<void>;
  log: Logger;
};

export async function finalizeSuccessfulJob(
  options: FinalizeSuccessfulJobOptions
): Promise<CompletionAckResult>;
```

The function has one intentional exception boundary:

- `uploadFinalArtifacts`, when provided, runs first and may throw. At that
  point the job is not completion-eligible, so its caller may report failure.
- After uploads succeed, the job is completion-eligible. Every remaining error
  is captured, logged, and returned; no post-eligibility exception escapes.

The implementation order inside the function is:

```text
await uploadFinalArtifacts()     # may throw; still eligible for /failed

# completion-eligible begins here
capture(await stopHeartbeat())
capture(await reportCompletion())
capture(await cleanup())
return Acknowledged or Pending
```

`reportFailed()` is not imported by this module. This is a structural
guarantee: after completion eligibility, the finalization seam has no way to
report the job as failed.

### Why callbacks are used

The three existing output modes differ only in how final artifacts are
uploaded and cleaned up. Small callbacks preserve those existing mechanisms
without teaching the new module about S3, sync configuration, directories, or
the Kelpie API client.

The callbacks also make the state transition directly unit-testable without
network, filesystem, or process fixtures. This is dependency injection at the
single side-effect boundary, not a new framework or abstraction hierarchy.

### Call-site behavior

Each successful path calls `finalizeSuccessfulJob()` exactly once:

- **No final sync:** omit `uploadFinalArtifacts` and `cleanup`.
- **Legacy output:** upload the moved output directory in
  `uploadFinalArtifacts`; remove it in `cleanup`.
- **`sync.after`:** move and upload every configured directory in
  `uploadFinalArtifacts`; clear moved directories in `cleanup`.

The existing chained `Promise.all(...).then(...).catch(...)` flow is replaced
with ordinary `await` control flow so the upload boundary is explicit.

After the function returns:

- `Acknowledged` maps to `observedExitAction="completed"`.
- `Pending` maps to `observedExitAction="completion_pending"` and copies the
  completion error into `observedExitError`.

If `uploadFinalArtifacts` throws, the existing outer failure path sets
`observedExitAction="failed"`, calls `/failed`, and stops heartbeat. This is
correct because required output is not durable.

### Heartbeat-stop failure

`JobHeartbeat.stop()` is designed to be terminal and idempotent. The
finalization seam still treats it defensively:

- Capture and log a stop exception.
- Continue to attempt `/completed`.
- If `/completed` succeeds, return `Acknowledged`; remote terminal state is
  authoritative.
- If `/completed` also fails, return `Pending` with both errors.
- Never call `/failed` for a heartbeat-control error after payload success.

### Local state

`reportCompleted()` currently records local state as completed before making
the HTTP request. That behavior remains unchanged. Local state therefore
records the immutable payload/final-upload outcome even when remote
acknowledgement is pending.

No new persisted local state is introduced. Salad containers are ephemeral,
so local persistence would not be a durable completion queue.

## State Flow

```text
                         +---------------------+
                         | payload running     |
                         +----------+----------+
                                    |
                    +---------------+---------------+
                    |                               |
              exit nonzero                     exit zero
                    |                               |
                    v                               v
             report /failed              upload required artifacts
                                                    |
                                      +-------------+-------------+
                                      |                           |
                                upload fails                upload succeeds
                                      |                           |
                                      v                           v
                               report /failed             completion-eligible
                                                                  |
                                                          stop heartbeat
                                                                  |
                                                          POST /completed
                                                                  |
                                             +--------------------+--------------------+
                                             |                                         |
                                      acknowledged                              retries exhausted
                                             |                                         |
                                             v                                         v
                                exit_action=completed                exit_action=completion_pending
                                                                                 never /failed
                                                                                       |
                                                                       stale-heartbeat reassignment
                                                                                       |
                                                                          _SUCCESS startup skip
                                                                                       |
                                                                              retry /completed
```

## Failure Matrix

| Condition | Completion-eligible | API action | Worker outcome |
| --- | --- | --- | --- |
| Payload exits nonzero | No | `/failed` | `failed` |
| Required final upload fails | No | `/failed` | `failed` |
| Payload and uploads succeed; `/completed` succeeds | Yes | `/completed` | `completed` |
| Payload and uploads succeed; `/completed` exhausts retries | Yes | Never `/failed` | `completion_pending` |
| Heartbeat stop fails; `/completed` succeeds | Yes | `/completed` | `completed`, log stop error |
| Heartbeat stop and `/completed` both fail | Yes | Never `/failed` | `completion_pending`, log both |
| Cleanup fails after `/completed` succeeds | Yes | No additional status call | Preserve `completed` |
| Cleanup fails while acknowledgement is pending | Yes | Never `/failed` | Preserve `completion_pending` |
| Remote cancellation while payload runs | No | No `/failed` | `canceled` |

## Recovery Behavior

Kelpie documentation states that a running job whose heartbeat is older than
twice its heartbeat interval is considered interrupted and is handed to an
eligible worker. An interruption does not increment `num_failures`.

For the production docking configuration with a 30-second heartbeat interval,
reassignment becomes eligible after approximately 60 seconds without a
heartbeat. Exact dispatch time also depends on worker polling and API
availability.

There are two possible outcomes after a completion response is lost:

### The API committed the transition

If `/completed` succeeded server-side but its response was lost, the job is
already terminal. No reassignment occurs. The worker may log
`completion_pending`, but the next status read shows `completed`.

### The API did not commit the transition

The job remains `running` with a stale heartbeat. After the control plane
recovers, Kelpie reassigns it. The docking command checks `_SUCCESS`, exits
zero, and makes another completion request without repeating docking.

This is at-least-once completion reporting. It does not claim exactly-once
delivery.

## Observability

### Structured pending event

Emit one error-level event when acknowledgement retries are exhausted:

```text
msg=completion_ack_pending
completion_ack_status=pending
report_failure=false
completion_report_error=...
heartbeat_stop_error=...        # optional
cleanup_error=...               # optional
job_id=...
kelpie_job_attempt_id=...
machine_id=...
container_group_id=...
run_name=...
shard=...
```

The child logger already supplies job, attempt, run, and shard context. The
new module adds only outcome-specific fields.

### Exit event

`kelpie_job_exit` uses:

```text
exit_code=0
exit_action=completion_pending
error=<completion report error>
```

The critical invariant can then be monitored directly:

```apl
['docking']
| where source == 'kelpie_worker'
    and msg == 'kelpie_job_exit'
    and exit_code == 0
    and exit_action == 'failed'
```

Expected post-rollout result: zero.

Pending acknowledgements remain visible with:

```apl
['docking']
| where source == 'kelpie_worker'
    and msg == 'completion_ack_pending'
| summarize events=count(), jobs=dcount(job_id), machines=dcount(machine_id)
    by bin(_time, 5m)
```

## Public Interfaces

No external interface changes:

- No Kelpie job JSON changes.
- No API endpoint changes.
- No new remote job status.
- No environment-variable changes.
- No Salad container-group changes.
- No docking tranche-spec changes.

Internal additions:

- `CompletionAckStatus` enum.
- `CompletionAckResult` discriminated union.
- `finalizeSuccessfulJob()` successful-finalization seam.
- `completion_pending` as a structured `kelpie_job_exit.exit_action` value.

Consumers that aggregate `exit_action` must tolerate the new value. It is
telemetry, not a wire-level Kelpie status.

## Alternatives Considered

### Catch only the known `/completed` exception

Rejected. It fixes the confirmed no-sync stack trace but leaves the
`sync.after` shared catch and post-eligibility cleanup exceptions able to call
`/failed`. It also depends on error-message matching.

### Add a `CompletionReportError` exception type

Rejected as the primary design. A typed exception is better than string
matching, but every present and future outer catch must remember to preserve
it. A non-throwing post-eligibility seam makes the invariant structural.

### Make `reportCompleted()` swallow every error

Rejected. Callers would lose the distinction between acknowledged and pending
completion, and `kelpie_job_exit` would incorrectly claim completion was
confirmed.

### Retry `/completed` indefinitely

Rejected. It retains paid workers during a control-plane outage and can block a
tranche at the queue terminal-state barrier. It recreates the operational
failure this work is intended to eliminate.

### Recreate or reallocate the Salad worker immediately

Rejected for the first fix. Recreation does not update Kelpie queue state and
adds image/model startup cost. The worker is not known to be unhealthy merely
because D1 or the API was unavailable.

### Add a second durable queue

Deferred. A generic completion outbox is valid architecture, but adds another
service, credentials, delivery semantics, and monitoring. Docking already has
durable `_SUCCESS` output and documented stale-heartbeat recovery.

### Add a tranche reconciliation process

Deferred until the Kelpie API exposes and documents an idempotent
administrative completion endpoint. The current worker endpoint includes
machine and container-group identity and has no public contract allowing an
external reconciler to repair an old lease.

## Test Plan

### Unit tests for `finalizeSuccessfulJob()`

1. Upload, heartbeat stop, completion report, and cleanup run in that order.
2. Upload failure rejects before heartbeat stop and completion report.
3. Completion success returns `Acknowledged`.
4. Completion failure returns `Pending` and does not throw.
5. Heartbeat-stop failure still attempts completion.
6. Heartbeat-stop failure plus completion success returns `Acknowledged`.
7. Heartbeat-stop failure plus completion failure returns `Pending` with both
   errors.
8. Cleanup failure is captured and cannot change `Acknowledged` to failure.
9. Cleanup failure is captured and cannot change `Pending` to failure.
10. The pending path emits exactly one structured error event.

The module cannot call `reportFailed()` because it does not import or receive
that function. Unit tests therefore verify behavior without mocking a failure
reporter.

### Worker integration tests

1. A no-sync command exits zero while `/completed` returns HTTP 500 through all
   retries:
   - `/failed` request count is zero;
   - heartbeat starts once and stops once;
   - no post-finalization heartbeat is sent;
   - `completion_ack_pending` is emitted once;
   - `kelpie_job_exit` reports exit code zero and `completion_pending`.
2. The same job is served again, its application-level success marker causes a
   fast zero exit, and the next `/completed` succeeds.
3. A required-upload failure still calls `/failed` exactly once.
4. A `sync.after` upload succeeds while `/completed` fails; `/failed` remains
   zero and the outcome is pending.
5. Post-upload cleanup failure never calls `/failed`.
6. Existing heartbeat race and cancellation integration tests remain green.

### Regression acceptance

- `npm test` passes.
- `npm run build` passes.
- Source search confirms every `reportFailed()` call is reachable only before
  completion eligibility or from a genuine nonzero payload outcome.
- No completion transport exception can reach the generic workload-failure
  catch.

## Implementation Plan

1. Add `src/completion.ts` and focused unit tests.
2. Refactor the three successful paths in `src/index.ts` to use the shared seam.
3. Replace the `sync.after` promise chain with explicit `await` boundaries.
4. Thread `CompletionAckResult` into `kelpie_job_exit` telemetry.
5. Add integration coverage for completion outage and redelivery.
6. Run the full test/build suite.
7. Commit implementation separately from this design document.

## Rollout Plan

1. Merge and build a new Kelpie binary/image.
2. Patch the existing Salad container group so new workers pull the new digest.
3. Do not scale the group down solely for rollout; running workers update on
   their normal recreate cycle.
4. Run a cloud test whose mock completion endpoint fails through the configured
   retries.
5. Verify Axiom receives `completion_ack_pending` and no corresponding
   `/failed` call.
6. Verify redelivery hits the application's existing-output path and then
   reaches `completed`.
7. Monitor for:
   - `exit_code=0 AND exit_action=failed` equal to zero;
   - pending acknowledgement rate;
   - time from pending event to remote completed state;
   - repeated pending events for one job;
   - stale running jobs with durable output.

## Future Reconciliation Service

A periodic reconciler may eventually compare Kelpie job state with durable R2
output and repair stale jobs. It should not call the current worker-only
completion endpoint without an explicit server contract.

The required future API properties are:

- Administrative authentication independent of a live Salad worker lease.
- Idempotent completion by job ID and attempt or proof identity.
- Conditional state transition that cannot revive or overwrite terminal jobs.
- `already_completed` treated as success.
- Explicit handling of canceled, failed, or reassigned jobs.
- Audit fields identifying the reconciler and durable output proof.

Until those guarantees exist, documented stale-heartbeat reassignment plus
application `_SUCCESS` is the supported recovery path.

## Acceptance Criteria

The implementation is complete when all of the following are true:

1. A zero-exit, fully uploaded job never calls `/failed`, regardless of any
   later heartbeat, completion-report, or cleanup error.
2. A completion acknowledgement failure is visible as `completion_pending`.
3. Heartbeat stops exactly once and cannot restart.
4. Required-upload failure and nonzero payload exit retain existing failure
   behavior.
5. A reassigned docking shard with `_SUCCESS` performs no docking and can
   acknowledge completion.
6. The full Kelpie test suite passes.
7. Design documentation and implementation are separate commits.
