import assert from "node:assert/strict";
import test from "node:test";

import { Logger } from "pino";

import {
  CompletionAckStatus,
  finalizeSuccessfulJob,
} from "./completion";

type LogEntry = {
  fields: Record<string, unknown>;
  message: string;
};

function recordingLogger(entries: LogEntry[]): Logger {
  return {
    error(fields: Record<string, unknown>, message: string) {
      entries.push({ fields, message });
    },
  } as unknown as Logger;
}

test("finalizes successful work in upload, heartbeat, report, cleanup order", async () => {
  const calls: string[] = [];
  const result = await finalizeSuccessfulJob({
    uploadFinalArtifacts: async () => {
      calls.push("upload");
    },
    stopHeartbeat: async () => {
      calls.push("stop-heartbeat");
    },
    reportCompletion: async () => {
      calls.push("report-completion");
    },
    cleanup: async () => {
      calls.push("cleanup");
    },
    log: recordingLogger([]),
  });

  assert.deepEqual(calls, [
    "upload",
    "stop-heartbeat",
    "report-completion",
    "cleanup",
  ]);
  assert.deepEqual(result, {
    status: CompletionAckStatus.Acknowledged,
    heartbeatStopError: undefined,
    cleanupError: undefined,
  });
});

test("upload failure escapes before completion eligibility", async () => {
  const calls: string[] = [];

  await assert.rejects(
    finalizeSuccessfulJob({
      uploadFinalArtifacts: async () => {
        calls.push("upload");
        throw new Error("upload unavailable");
      },
      stopHeartbeat: async () => {
        calls.push("stop-heartbeat");
      },
      reportCompletion: async () => {
        calls.push("report-completion");
      },
      cleanup: async () => {
        calls.push("cleanup");
      },
      log: recordingLogger([]),
    }),
    /upload unavailable/
  );

  assert.deepEqual(calls, ["upload"]);
});

test("completion report failure returns pending and does not throw", async () => {
  const calls: string[] = [];
  const logs: LogEntry[] = [];
  const result = await finalizeSuccessfulJob({
    stopHeartbeat: async () => {
      calls.push("stop-heartbeat");
    },
    reportCompletion: async () => {
      calls.push("report-completion");
      throw new Error("completion API unavailable");
    },
    cleanup: async () => {
      calls.push("cleanup");
    },
    log: recordingLogger(logs),
  });

  assert.deepEqual(calls, [
    "stop-heartbeat",
    "report-completion",
    "cleanup",
  ]);
  assert.deepEqual(result, {
    status: CompletionAckStatus.Pending,
    completionReportError: "completion API unavailable",
    heartbeatStopError: undefined,
    cleanupError: undefined,
  });
  assert.equal(logs.length, 1);
  assert.deepEqual(logs[0], {
    fields: {
      completion_ack_status: CompletionAckStatus.Pending,
      report_failure: false,
      completion_report_error: "completion API unavailable",
      heartbeat_stop_error: undefined,
      cleanup_error: undefined,
    },
    message: "completion_ack_pending",
  });
});

test("heartbeat stop failure still attempts and acknowledges completion", async () => {
  const calls: string[] = [];
  const result = await finalizeSuccessfulJob({
    stopHeartbeat: async () => {
      calls.push("stop-heartbeat");
      throw new Error("heartbeat stop failed");
    },
    reportCompletion: async () => {
      calls.push("report-completion");
    },
    log: recordingLogger([]),
  });

  assert.deepEqual(calls, ["stop-heartbeat", "report-completion"]);
  assert.deepEqual(result, {
    status: CompletionAckStatus.Acknowledged,
    heartbeatStopError: "heartbeat stop failed",
    cleanupError: undefined,
  });
});

test("heartbeat and completion failures are both preserved as pending", async () => {
  const logs: LogEntry[] = [];
  const result = await finalizeSuccessfulJob({
    stopHeartbeat: async () => {
      throw new Error("heartbeat stop failed");
    },
    reportCompletion: async () => {
      throw new Error("completion report failed");
    },
    log: recordingLogger(logs),
  });

  assert.deepEqual(result, {
    status: CompletionAckStatus.Pending,
    completionReportError: "completion report failed",
    heartbeatStopError: "heartbeat stop failed",
    cleanupError: undefined,
  });
  assert.equal(logs[logs.length - 1]?.message, "completion_ack_pending");
  assert.equal(
    logs[logs.length - 1]?.fields.heartbeat_stop_error,
    "heartbeat stop failed"
  );
});

test("cleanup failure cannot change acknowledged completion", async () => {
  const result = await finalizeSuccessfulJob({
    stopHeartbeat: async () => {},
    reportCompletion: async () => {},
    cleanup: async () => {
      throw new Error("cleanup failed");
    },
    log: recordingLogger([]),
  });

  assert.deepEqual(result, {
    status: CompletionAckStatus.Acknowledged,
    heartbeatStopError: undefined,
    cleanupError: "cleanup failed",
  });
});

test("cleanup failure cannot change pending completion", async () => {
  const logs: LogEntry[] = [];
  const result = await finalizeSuccessfulJob({
    stopHeartbeat: async () => {},
    reportCompletion: async () => {
      throw new Error("completion report failed");
    },
    cleanup: async () => {
      throw new Error("cleanup failed");
    },
    log: recordingLogger(logs),
  });

  assert.deepEqual(result, {
    status: CompletionAckStatus.Pending,
    completionReportError: "completion report failed",
    heartbeatStopError: undefined,
    cleanupError: "cleanup failed",
  });
  assert.equal(
    logs.filter((entry) => entry.message === "completion_ack_pending").length,
    1
  );
  assert.equal(
    logs[logs.length - 1]?.fields.cleanup_error,
    "cleanup failed"
  );
});
