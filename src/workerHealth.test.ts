import assert from "node:assert/strict";
import test from "node:test";

import {
  decideWorkerRecovery,
  WorkerHealthSignal,
  WorkerProcessExitCode,
  WorkerRecoveryAction,
} from "./workerHealth";

test("worker-unhealthy exit requests recreation", () => {
  assert.deepEqual(
    decideWorkerRecovery(WorkerProcessExitCode.Unhealthy, false),
    {
      signal: WorkerHealthSignal.UnhealthyExit,
      action: WorkerRecoveryAction.Recreate,
      reason: "job_exit_75",
    }
  );
});

test("ordinary failures do not request immediate reallocation", () => {
  assert.deepEqual(decideWorkerRecovery(1, false), {
    signal: WorkerHealthSignal.None,
    action: WorkerRecoveryAction.None,
    reason: null,
  });
});

test("successful and canceled work do not request reallocation", () => {
  assert.equal(
    decideWorkerRecovery(0, false).action,
    WorkerRecoveryAction.None
  );
  assert.equal(
    decideWorkerRecovery(WorkerProcessExitCode.Unhealthy, true).action,
    WorkerRecoveryAction.None
  );
});
