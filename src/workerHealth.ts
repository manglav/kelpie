export enum WorkerProcessExitCode {
  Unhealthy = 75,
}

export enum WorkerHealthSignal {
  None = "none",
  UnhealthyExit = "worker_unhealthy_exit",
}

export enum WorkerRecoveryAction {
  None = "none",
  Recreate = "recreate",
}

export type WorkerRecoveryDecision = {
  signal: WorkerHealthSignal;
  action: WorkerRecoveryAction;
  reason: string | null;
};

export function decideWorkerRecovery(
  exitCode: number | null,
  jobWasCanceled: boolean
): WorkerRecoveryDecision {
  if (!jobWasCanceled && exitCode === WorkerProcessExitCode.Unhealthy) {
    return {
      signal: WorkerHealthSignal.UnhealthyExit,
      action: WorkerRecoveryAction.Recreate,
      reason: "job_exit_75",
    };
  }

  return {
    signal: WorkerHealthSignal.None,
    action: WorkerRecoveryAction.None,
    reason: null,
  };
}
