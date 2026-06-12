export type JobExitAction = "completed" | "failed" | "canceled";

export type JobExitDecision = {
  action: JobExitAction;
  reportFailure: boolean;
  exitCode: number | null;
};

export function decideJobExitAction(
  exitCode: number | null,
  jobWasCanceled: boolean
): JobExitDecision {
  if (jobWasCanceled) {
    return {
      action: "canceled",
      reportFailure: false,
      exitCode,
    };
  }

  if (exitCode === 0) {
    return {
      action: "completed",
      reportFailure: false,
      exitCode,
    };
  }

  return {
    action: "failed",
    reportFailure: true,
    exitCode,
  };
}
