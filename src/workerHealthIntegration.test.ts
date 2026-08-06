import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  WorkerHealthSignal,
  WorkerProcessExitCode,
  WorkerRecoveryAction,
} from "./workerHealth";

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}

async function exerciseUnhealthyExit(failFailureAck: boolean): Promise<{
  completedCount: number;
  failedCount: number;
  recreateCount: number;
  reallocateCount: number;
  stdout: string;
}> {
  const runDir = await mkdtemp(path.join(os.tmpdir(), "kelpie-health-"));
  let workServed = false;
  let completedCount = 0;
  let failedCount = 0;
  let recreateCount = 0;
  let reallocateCount = 0;

  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    const requestUrl = new URL(req.url ?? "/", "http://127.0.0.1");

    if (req.method === "GET" && requestUrl.pathname === "/work") {
      if (workServed) {
        sendJson(res, 200, []);
        return;
      }
      workServed = true;
      sendJson(res, 200, [
        {
          id: "job-worker-unhealthy-1",
          user_id: "user-1",
          status: "running",
          created: new Date().toISOString(),
          num_failures: 0,
          machine_id: "machine-health-1",
          command: process.execPath,
          arguments: ["-e", `process.exit(${WorkerProcessExitCode.Unhealthy})`],
          environment: {},
          heartbeat_interval: 0.05,
          max_failures: 3,
          container_group_id: "group-health-1",
          sync: {},
        },
      ]);
      return;
    }

    if (
      req.method === "POST" &&
      requestUrl.pathname === "/jobs/job-worker-unhealthy-1/heartbeat"
    ) {
      sendJson(res, 200, { status: "running" });
      return;
    }

    if (
      req.method === "POST" &&
      requestUrl.pathname === "/jobs/job-worker-unhealthy-1/completed"
    ) {
      completedCount++;
      sendJson(res, 200, {});
      return;
    }

    if (
      req.method === "POST" &&
      requestUrl.pathname === "/jobs/job-worker-unhealthy-1/failed"
    ) {
      failedCount++;
      sendJson(
        res,
        failFailureAck ? 503 : 200,
        failFailureAck ? { error: "simulated queue outage" } : {}
      );
      return;
    }

    if (req.method === "PUT" && requestUrl.pathname === "/v1/deletion-cost") {
      res.writeHead(204);
      res.end();
      return;
    }

    if (req.method === "POST" && requestUrl.pathname === "/v1/recreate") {
      recreateCount++;
      res.writeHead(204);
      res.end();
      return;
    }

    if (req.method === "POST" && requestUrl.pathname === "/v1/reallocate") {
      reallocateCount++;
      res.writeHead(204);
      res.end();
      return;
    }

    sendJson(res, 404, {
      error: `Unhandled ${req.method} ${requestUrl.pathname}`,
    });
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const baseUrl = `http://127.0.0.1:${address.port}`;

  try {
    const worker = spawn(process.execPath, [path.join(__dirname, "index.js")], {
      stdio: ["ignore", "pipe", "pipe"],
      env: {
        ...process.env,
        KELPIE_API_KEY: "test-key",
        KELPIE_API_URL: baseUrl,
        SALAD_IMDS_URL: baseUrl,
        INPUT_DIR: path.join(runDir, "input"),
        OUTPUT_DIR: path.join(runDir, "output"),
        CHECKPOINT_DIR: path.join(runDir, "checkpoint"),
        KELPIE_STATE_FILE: path.join(runDir, "kelpie-state.json"),
        KELPIE_RECREATE_BETWEEN_JOBS: "false",
        KELPIE_RECREATE_EVERY_N_JOBS: "0",
        SALAD_MACHINE_ID: "machine-health-1",
        SALAD_CONTAINER_GROUP_ID: "group-health-1",
        SCREENING_WORKER_START_ID: "worker-start-health-1",
        MAX_JOB_FAILURES: "1",
        MAX_RETRIES: "1",
        MAX_TIME_WITH_NO_WORK_S: "0",
      },
    });

    let stdout = "";
    let stderr = "";
    worker.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    worker.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    const exitCode = await new Promise<number | null>((resolve, reject) => {
      const timer = setTimeout(() => {
        worker.kill("SIGKILL");
        reject(
          new Error(`Kelpie worker timed out. stdout=${stdout} stderr=${stderr}`)
        );
      }, 10_000);
      worker.on("exit", (code) => {
        clearTimeout(timer);
        resolve(code);
      });
    });

    assert.equal(exitCode, 0, `stdout=${stdout} stderr=${stderr}`);
    return {
      completedCount,
      failedCount,
      recreateCount,
      reallocateCount,
      stdout,
    };
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(runDir, { recursive: true, force: true });
  }
}

for (const failFailureAck of [false, true]) {
  test(
    `worker-unhealthy exit reallocates once when failure acknowledgement ${
      failFailureAck ? "fails" : "succeeds"
    }`,
    async () => {
      const result = await exerciseUnhealthyExit(failFailureAck);
      assert.equal(result.completedCount, 0);
      assert.equal(result.failedCount, 1);
      assert.equal(result.recreateCount, 0);
      assert.equal(result.reallocateCount, 1);

      const events = result.stdout
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line) as Record<string, unknown>);
      const terminal = events.find((event) => event.msg === "kelpie_job_exit");
      assert.ok(terminal);
      assert.equal(terminal.exit_code, WorkerProcessExitCode.Unhealthy);
      assert.equal(terminal.exit_action, "failed");
      assert.equal(
        terminal.worker_health_signal,
        WorkerHealthSignal.UnhealthyExit
      );
      assert.equal(terminal.recovery_action, WorkerRecoveryAction.Reallocate);
      assert.equal(terminal.recovery_reason, "job_exit_75");
      assert.equal(
        terminal.failure_ack_status,
        failFailureAck ? "pending" : "acknowledged"
      );
      assert.equal(
        events.filter((event) => event.msg === "Heartbeat started.").length,
        1
      );
      assert.equal(
        events.filter((event) => event.msg === "Heartbeat stopped.").length,
        1
      );
      assert.equal(
        events.filter((event) => event.msg === "failure_ack_pending").length,
        failFailureAck ? 1 : 0
      );
    }
  );
}
