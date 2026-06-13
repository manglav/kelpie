# Kelpie Canceled-Job Process Group Design

## Problem

Kelpie currently launches a job command as one direct child process and sends
cancel signals only to that direct child. Some jobs launch nested subprocesses.
For screening, the relevant tree is:

```text
kelpie
  -> screen_shard.py
      -> dock.py
          -> _dock_worker.py
```

If cancellation reaches only `screen_shard.py`, descendants can keep running and
using the GPU while Kelpie moves on to another job. That creates duplicate work
and can waste GPU time.

## Desired Contract

Kelpie owns the lifecycle of one submitted job. A submitted job must be treated
as a process group, not as only one direct child process.

The core invariant is:

```text
If Kelpie observes remote cancellation, this worker does not accept another job
in the same container.
```

There are two independent guarantees:

1. Stop the current job process tree locally.
2. Recreate the container after any remotely canceled job, when enabled.

Process-group signaling stops GPU work quickly. Container recreate resets worker
state before the next job.

## Basic Process Group Behavior

Kelpie should launch every job in a new Unix process group:

```ts
const child = spawn(command, args, {
  env,
  stdio: "inherit",
  detached: true,
});
```

On Linux, `detached: true` makes the child the leader of a new process group and
session. The child's PID is therefore the process group ID:

```ts
const pid = child.pid;
const pgid = child.pid;
```

Kelpie should store this as a single running-job object:

```ts
type RunningJob = {
  child: ChildProcess;
  pid: number;
  pgid: number;
};
```

If a job is running, `pid` and `pgid` must always be present. If no job is
running, the state is `null`.

To signal the whole job tree:

```ts
process.kill(-running.pgid, signal);
```

The negative PID is the Unix process-group signaling convention.

## Remote Cancellation Flow

When heartbeat reports that a job was canceled remotely:

```text
remote cancel observed
  -> mark jobWasCanceled = true
  -> send SIGINT to job process group
  -> wait KELPIE_CANCEL_SIGINT_GRACE_S

  -> if process group is still alive:
       send SIGTERM
       wait KELPIE_CANCEL_SIGTERM_GRACE_S

  -> if process group is still alive:
       send SIGKILL
       wait KELPIE_CANCEL_SIGKILL_GRACE_S

  -> record cleanup outcome:
       group_empty=true/false
       final_signal=SIGINT/SIGTERM/SIGKILL/none
       cleanup_elapsed_ms=...

  -> because jobWasCanceled && KELPIE_RECREATE_AFTER_CANCELED_JOB=true:
       wait for uploads if any
       call IMDS /recreate
       sleep 1s
       break worker loop
```

Container recreate is not only for the worst case. With
`KELPIE_RECREATE_AFTER_CANCELED_JOB=true`, recreate happens after every remotely
canceled job.

| Cleanup outcome | Recreate behavior |
| --- | --- |
| Job exits after `SIGINT` | Recreate container |
| Job exits after `SIGTERM` | Recreate container |
| Job exits after `SIGKILL` | Recreate container |
| Job still appears alive after `SIGKILL` | Recreate container and log `group_empty=false` |

## Late Cancellation After Local Exit

Remote cancellation is asynchronous. At high scale, the scheduler can hand the
same job to more than one worker and then cancel one attempt after another
attempt has already won. A worker can therefore observe cancellation after its
local job process has already exited.

This is common for fast resume/no-op shards:

```text
worker receives job
  -> job sees output marker already exists
  -> job exits 0 quickly
  -> heartbeat later observes remote status=canceled
```

This must not be treated the same as canceling a live process. The intended
semantics are:

```text
if heartbeat reports canceled and a job process is still running:
  -> active remote cancellation
  -> mark jobWasCanceled=true
  -> signal process group
  -> emit bounded wait logs while the process group exists
  -> recreate if KELPIE_RECREATE_AFTER_CANCELED_JOB=true

if heartbeat reports canceled after the local job process already exited:
  -> late remote cancellation after process exit
  -> do not mark jobWasCanceled=true
  -> do not start cancellation wait logs
  -> do not recreate solely because of this late cancel
  -> log late_remote_cancellation_after_process_exit
  -> allow normal completion/reporting path to finish
```

This distinction matters because the log message
`Remote cancellation still waiting for process exit` must mean that a live local
process group is still being waited on. It must not be emitted for already-dead
processes.

The worker must also clear its running-process state as soon as the child emits
`exit`. If stale process metadata remains until the end of job bookkeeping, a
late cancel can be mislabeled as `cancel_signal_target=process_group` even when
the process group is already empty.

Useful logs for this race:

```text
remote_cancellation_observed
late_remote_cancellation_after_process_exit
job_process_group_cleanup_complete reason=late_remote_cancel_after_process_exit
kelpie_job_exit late_cancellation_after_process_exit=true
```

The expected cleanup outcome for a late cancel is `group_empty=true` with no
signal escalation.

## Environment Variables

Existing relevant variables:

```text
KELPIE_RECREATE_BETWEEN_JOBS=false
KELPIE_RECREATE_EVERY_N_JOBS=0
KELPIE_CANCEL_PROGRESS_LOG_INTERVAL_S=10
MAX_TIME_WITH_NO_WORK_S=0
HEARTBEAT_INTERVAL_S=10
```

Add these variables for cancellation behavior:

```text
KELPIE_RECREATE_AFTER_CANCELED_JOB=false
KELPIE_CANCEL_SIGINT_GRACE_S=15
KELPIE_CANCEL_SIGTERM_GRACE_S=15
KELPIE_CANCEL_SIGKILL_GRACE_S=3
```

For screening workers, use:

```text
KELPIE_RECREATE_AFTER_CANCELED_JOB=true
KELPIE_CANCEL_SIGINT_GRACE_S=15
KELPIE_CANCEL_SIGTERM_GRACE_S=15
KELPIE_CANCEL_SIGKILL_GRACE_S=3
```

Process-group job supervision should not be optional. Only timing and
recreate-after-cancel policy should be configurable.

## Logging

Kelpie should emit structured logs for cancellation and cleanup:

```text
job_process_group_started
remote_cancellation_observed
job_process_group_signal_sent
job_process_group_signal_wait
job_process_group_cleanup_complete
late_remote_cancellation_after_process_exit
container_recreate_requested
work_exited_after_remote_cancellation
```

Important fields:

```text
job_id
pid
pgid
signal
reason
signal_sent
group_empty
final_signal
cleanup_elapsed_ms
cancel_to_exit_ms
recreate_after_canceled_job
recreate_reason
late_cancellation_after_process_exit
```

## Job Outcome Semantics

If remote cancellation was observed, job outcome remains canceled even if the
process exits nonzero after receiving a signal. Canceled jobs should not be
reported as failed.

Examples:

| Remote cancel observed | Process exit | Kelpie outcome |
| --- | --- | --- |
| Yes | `0` | canceled |
| Yes | `130` | canceled |
| Yes | signal exit | canceled |
| No | `0` | completed |
| No | nonzero | failed |

A late remote cancellation after local process exit is not considered an active
remote cancellation for local job outcome purposes. The local process has already
finished, so Kelpie should report/log the normal local outcome and include
`late_cancellation_after_process_exit=true` for observability.

## Test Plan

Add tests for:

1. Job launch creates a new process group and stores required `pid`/`pgid`.
2. Remote cancel signals the process group, not only the direct child.
3. A child/grandchild process receives cancellation when the group is signaled.
4. Cleanup escalates `SIGINT -> SIGTERM -> SIGKILL` when a process ignores
   earlier signals.
5. Cleanup records `group_empty=false` if the group still appears alive after
   `SIGKILL`.
6. A remotely canceled job exits nonzero but remains canceled, not failed.
7. With `KELPIE_RECREATE_AFTER_CANCELED_JOB=true`, any remotely canceled job
   requests IMDS recreate, regardless of whether cleanup ended at `SIGINT`,
   `SIGTERM`, `SIGKILL`, or `group_empty=false`.
8. With `KELPIE_RECREATE_AFTER_CANCELED_JOB=false`, canceled-job recreate is not
   requested.
9. Existing `KELPIE_RECREATE_BETWEEN_JOBS` and
   `KELPIE_RECREATE_EVERY_N_JOBS` behavior remains unchanged for normal jobs.
10. A cancellation observed after local process exit logs
    `late_remote_cancellation_after_process_exit`, does not emit active wait
    logs, does not recreate solely due to the late cancel, and preserves the
    normal local completion path.

## Implementation Order

1. Keep `KELPIE_RECREATE_EVERY_N_JOBS` support as its own commit.
2. Add process-group launch and process-group cancel to `CommandExecutor`.
3. Add bounded escalation and cleanup outcome logging.
4. Add `KELPIE_RECREATE_AFTER_CANCELED_JOB` and request recreate after any
   remotely canceled job when enabled.
5. Add tests for process-group cancellation, escalation, and canceled-job
   recreate policy.
