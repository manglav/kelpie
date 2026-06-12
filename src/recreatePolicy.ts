export type RecreateReason = "between_jobs" | "every_n_jobs";

export type RecreateDecision =
  | {
      shouldRecreate: true;
      reason: RecreateReason;
    }
  | {
      shouldRecreate: false;
      reason: null;
    };

export function parseRecreateEveryNJobs(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 0;
  }
  return parsed;
}

export function decideRecreateAfterJob({
  recreateBetweenJobs,
  recreateEveryNJobs,
  jobsSinceRecreate,
}: {
  recreateBetweenJobs: boolean;
  recreateEveryNJobs: number;
  jobsSinceRecreate: number;
}): RecreateDecision {
  if (recreateBetweenJobs) {
    return { shouldRecreate: true, reason: "between_jobs" };
  }

  if (recreateEveryNJobs > 0 && jobsSinceRecreate >= recreateEveryNJobs) {
    return { shouldRecreate: true, reason: "every_n_jobs" };
  }

  return { shouldRecreate: false, reason: null };
}
