# Kelpie Worker Control-Plane Reliability

## Context

The Kelpie API can intermittently return errors or take too long to respond when its backing Cloudflare services are degraded. For a paid Salad instance, an ambiguous or delayed `/work`, heartbeat, completion, or failure response can leave the worker idle or make assignment state difficult to observe.

This change strengthens the Kelpie worker's HTTP behavior without changing job semantics. It does not modify the Kelpie API, job submission, assignment rules, cancellation rules, autoscaling, artifact handling, or the order of lifecycle operations.

## Scope

The worker gains independent, environment-controlled settings for:

- Per-attempt HTTP timeouts.
- Maximum HTTP attempts.
- Capped exponential delay between attempts.
- The cadence for polling `/work` while idle.
- The cadence for sending heartbeats while a job is running.

The worker logs the effective non-secret settings at startup. Invalid values fail fast instead of silently producing a tight loop or an unbounded wait.

## Configuration

| Variable | Default | Meaning |
| --- | ---: | --- |
| `KELPIE_API_REQUEST_TIMEOUT_S` | `10` | Maximum duration of one Kelpie HTTP attempt. |
| `MAX_RETRIES` | `3` | Maximum attempts for one Kelpie API operation, including the first attempt. |
| `KELPIE_API_RETRY_INITIAL_DELAY_S` | `5` | Delay after the first failed attempt. |
| `KELPIE_API_RETRY_MAX_DELAY_S` | `30` | Maximum delay between attempts. |
| `KELPIE_WORK_POLL_INTERVAL_S` | `30` | Delay between idle `/work` polls and after exhausted `/work` attempts. |
| `KELPIE_JOB_HEARTBEAT_INTERVAL_S` | `30` | Delay between accepted job heartbeats. |

For compatibility, `HEARTBEAT_INTERVAL_S` is used as the work-poll interval only when `KELPIE_WORK_POLL_INTERVAL_S` is absent. It never overrides the new variable.

Retry delays are `5s`, `10s`, `20s`, then capped at `30s` with the defaults. There is no delay after the final failed attempt. Every attempt has its own timeout. Stopping a heartbeat aborts immediately rather than retrying a deliberately aborted request.

The submitted job's `heartbeat_interval` remains server-side lease metadata. It no longer controls how often this worker sends requests. Docking job submission should continue to use `heartbeat_interval=150`, yielding the existing 300-second stale-assignment threshold in the Kelpie API, while workers send heartbeats every 30 seconds.

## Invariants

The following behavior is intentionally unchanged:

1. A worker receives work from `GET /work`.
2. It executes only the assignment returned by the API.
3. Cancellation is observed from the heartbeat response and follows the existing process-stop sequence.
4. Successful completion remains: upload final artifacts, stop heartbeat, report completion, then clean up.
5. Failed jobs follow the existing failure-reporting path.
6. The worker does not claim, deduplicate, redistribute, or mutate queue state outside the existing API calls.

## Rollout and rollback

1. Build and test the Kelpie binary.
2. Pin the tested Kelpie commit in `docking_on_salad/scripts/build_screening_image.sh`.
3. Use the existing screening image build script to push an immutable image tag.
4. Use `scripts/refresh_screening_group_image.sh` to point container group `b9d0a7db-5c33-491a-af5d-dea2ed492dc2` at that immutable tag.
5. Merge the six timing variables into the group's existing environment dictionary; Salad replaces the whole dictionary on update, so all existing values must be preserved.
6. Start with a small canary before restoring normal scale.

Rollback is an image refresh back to the previous production image plus removal or restoration of the six timing overrides. The container group must not be scaled down as part of this process without explicit approval.
