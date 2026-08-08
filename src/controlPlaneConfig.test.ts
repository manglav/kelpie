import assert from "node:assert/strict";
import test from "node:test";

import {
  loadControlPlaneConfig,
  retryDelayMs,
} from "./controlPlaneConfig";

test("uses reliable control-plane defaults", () => {
  assert.deepEqual(loadControlPlaneConfig({}), {
    apiRequestTimeoutMs: 10_000,
    apiRetryInitialDelayMs: 5_000,
    apiRetryMaxDelayMs: 30_000,
    jobHeartbeatIntervalMs: 30_000,
    maxAttempts: 3,
    workPollIntervalMs: 30_000,
  });
});

test("reads explicit reliability overrides", () => {
  assert.deepEqual(
    loadControlPlaneConfig({
      KELPIE_API_REQUEST_TIMEOUT_S: "12.5",
      KELPIE_API_RETRY_INITIAL_DELAY_S: "2",
      KELPIE_API_RETRY_MAX_DELAY_S: "8",
      KELPIE_JOB_HEARTBEAT_INTERVAL_S: "20",
      KELPIE_WORK_POLL_INTERVAL_S: "15",
      MAX_RETRIES: "4",
    }),
    {
      apiRequestTimeoutMs: 12_500,
      apiRetryInitialDelayMs: 2_000,
      apiRetryMaxDelayMs: 8_000,
      jobHeartbeatIntervalMs: 20_000,
      maxAttempts: 4,
      workPollIntervalMs: 15_000,
    }
  );
});

test("uses legacy HEARTBEAT_INTERVAL_S only as a work-poll fallback", () => {
  assert.equal(
    loadControlPlaneConfig({ HEARTBEAT_INTERVAL_S: "7" })
      .workPollIntervalMs,
    7_000
  );
  assert.equal(
    loadControlPlaneConfig({
      HEARTBEAT_INTERVAL_S: "7",
      KELPIE_WORK_POLL_INTERVAL_S: "11",
    }).workPollIntervalMs,
    11_000
  );
});

test("rejects invalid reliability settings", () => {
  assert.throws(
    () => loadControlPlaneConfig({ KELPIE_API_REQUEST_TIMEOUT_S: "0" }),
    /KELPIE_API_REQUEST_TIMEOUT_S must be a positive number/
  );
  assert.throws(
    () => loadControlPlaneConfig({ MAX_RETRIES: "1.5" }),
    /MAX_RETRIES must be a positive integer/
  );
  assert.throws(
    () =>
      loadControlPlaneConfig({
        KELPIE_API_RETRY_INITIAL_DELAY_S: "10",
        KELPIE_API_RETRY_MAX_DELAY_S: "5",
      }),
    /KELPIE_API_RETRY_MAX_DELAY_S must be greater than or equal/
  );
});

test("uses capped exponential retry delays", () => {
  const retryConfig = {
    apiRetryInitialDelayMs: 5_000,
    apiRetryMaxDelayMs: 30_000,
  };
  assert.deepEqual(
    [1, 2, 3, 4, 5].map((attempt) => retryDelayMs(attempt, retryConfig)),
    [5_000, 10_000, 20_000, 30_000, 30_000]
  );
});
