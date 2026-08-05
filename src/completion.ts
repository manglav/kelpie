import { Logger } from "pino";

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

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export async function finalizeSuccessfulJob(
  options: FinalizeSuccessfulJobOptions
): Promise<CompletionAckResult> {
  await options.uploadFinalArtifacts?.();

  let heartbeatStopError: string | undefined;
  try {
    await options.stopHeartbeat();
  } catch (error: unknown) {
    heartbeatStopError = errorMessage(error);
    options.log.error(
      { heartbeat_stop_error: heartbeatStopError },
      "completion_heartbeat_stop_failed"
    );
  }

  let completionReportError: string | undefined;
  try {
    await options.reportCompletion();
  } catch (error: unknown) {
    completionReportError = errorMessage(error);
  }

  let cleanupError: string | undefined;
  try {
    await options.cleanup?.();
  } catch (error: unknown) {
    cleanupError = errorMessage(error);
    options.log.error(
      { cleanup_error: cleanupError },
      "completion_cleanup_failed"
    );
  }

  if (completionReportError) {
    options.log.error(
      {
        completion_ack_status: CompletionAckStatus.Pending,
        report_failure: false,
        completion_report_error: completionReportError,
        heartbeat_stop_error: heartbeatStopError,
        cleanup_error: cleanupError,
      },
      "completion_ack_pending"
    );
    return {
      status: CompletionAckStatus.Pending,
      completionReportError,
      heartbeatStopError,
      cleanupError,
    };
  }

  return {
    status: CompletionAckStatus.Acknowledged,
    heartbeatStopError,
    cleanupError,
  };
}
