import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}

async function runWorker({
  runDir,
  baseUrl,
  machineId,
  containerGroupId,
  recreateBetweenJobs,
  recreateEveryNJobs,
  timeoutMs = 10_000,
}: {
  runDir: string;
  baseUrl: string;
  machineId: string;
  containerGroupId: string;
  recreateBetweenJobs: boolean;
  recreateEveryNJobs: number;
  timeoutMs?: number;
}): Promise<string> {
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
      KELPIE_RECREATE_BETWEEN_JOBS: String(recreateBetweenJobs),
      KELPIE_RECREATE_EVERY_N_JOBS: String(recreateEveryNJobs),
      SALAD_MACHINE_ID: machineId,
      SALAD_CONTAINER_GROUP_ID: containerGroupId,
      KELPIE_JOB_HEARTBEAT_INTERVAL_S: "0.05",
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
    }, timeoutMs);
    worker.on("exit", (code) => {
      clearTimeout(timer);
      resolve(code);
    });
  });

  assert.equal(exitCode, 0, `stdout=${stdout} stderr=${stderr}`);
  return stdout;
}

test("completion acknowledgement failure cannot reclassify successful work", async () => {
  const runDir = await mkdtemp(path.join(os.tmpdir(), "kelpie-completion-"));

  let workServed = false;
  let heartbeatCount = 0;
  let heartbeatCountAtCompletion = 0;
  let completedCount = 0;
  let failedCount = 0;
  let recreateCount = 0;

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
          id: "job-completion-pending-1",
          user_id: "user-1",
          status: "running",
          created: new Date().toISOString(),
          num_failures: 0,
          machine_id: "machine-1",
          command: process.execPath,
          arguments: ["-e", "process.exit(0)"],
          environment: {},
          heartbeat_interval: 0.05,
          max_failures: 3,
          container_group_id: "group-1",
          sync: {},
        },
      ]);
      return;
    }

    if (
      req.method === "POST" &&
      requestUrl.pathname ===
        "/jobs/job-completion-pending-1/heartbeat"
    ) {
      heartbeatCount++;
      sendJson(res, 200, { status: "running" });
      return;
    }

    if (
      req.method === "POST" &&
      requestUrl.pathname ===
        "/jobs/job-completion-pending-1/completed"
    ) {
      completedCount++;
      heartbeatCountAtCompletion = heartbeatCount;
      sendJson(res, 503, { error: "simulated control-plane outage" });
      return;
    }

    if (
      req.method === "POST" &&
      requestUrl.pathname === "/jobs/job-completion-pending-1/failed"
    ) {
      failedCount++;
      sendJson(res, 200, {});
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

    sendJson(res, 404, {
      error: `Unhandled ${req.method} ${requestUrl.pathname}`,
    });
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const baseUrl = `http://127.0.0.1:${address.port}`;

  try {
    const stdout = await runWorker({
      runDir,
      baseUrl,
      machineId: "machine-completion-test",
      containerGroupId: "group-completion-test",
      recreateBetweenJobs: true,
      recreateEveryNJobs: 0,
    });
    assert.equal(completedCount, 1);
    assert.equal(failedCount, 0);
    assert.equal(recreateCount, 1);
    assert.equal(heartbeatCount, heartbeatCountAtCompletion);
    assert.equal((stdout.match(/Heartbeat started\./g) ?? []).length, 1);
    assert.equal((stdout.match(/Heartbeat stopped\./g) ?? []).length, 1);
    assert.match(stdout, /"msg":"completion_ack_pending"/);
    assert.match(stdout, /"report_failure":false/);
    assert.match(stdout, /"exit_action":"completion_pending"/);
    assert.doesNotMatch(stdout, /"exit_action":"failed"/);
    const pendingEvent = stdout
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as Record<string, unknown>)
      .find((event) => event.msg === "completion_ack_pending");
    assert.ok(pendingEvent);
    assert.equal(pendingEvent.job_id, "job-completion-pending-1");
    assert.equal(pendingEvent.machine_id, "machine-completion-test");
    assert.equal(pendingEvent.container_group_id, "group-completion-test");
    assert.equal(pendingEvent.completion_ack_status, "pending");
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(runDir, { recursive: true, force: true });
  }
});

test("pending completion can be redelivered and acknowledged without repeating work", async () => {
  const runDir = await mkdtemp(path.join(os.tmpdir(), "kelpie-redelivery-"));
  const childPath = path.join(runDir, "durable-success.js");
  const markerPath = path.join(runDir, "success-marker");
  const invocationPath = path.join(runDir, "invocations");
  const expensiveWorkPath = path.join(runDir, "expensive-work");
  await writeFile(
    childPath,
    `
      const fs = require("fs");
      fs.appendFileSync(process.env.KELPIE_TEST_INVOCATIONS, "run\\n");
      if (!fs.existsSync(process.env.KELPIE_TEST_SUCCESS_MARKER)) {
        fs.appendFileSync(process.env.KELPIE_TEST_EXPENSIVE_WORK, "run\\n");
        fs.writeFileSync(process.env.KELPIE_TEST_SUCCESS_MARKER, "done");
      }
    `
  );

  let workServedCount = 0;
  let completedCount = 0;
  let failedCount = 0;
  let recreateCount = 0;

  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    const requestUrl = new URL(req.url ?? "/", "http://127.0.0.1");

    if (req.method === "GET" && requestUrl.pathname === "/work") {
      if (workServedCount >= 2) {
        sendJson(res, 200, []);
        return;
      }
      workServedCount++;
      sendJson(res, 200, [
        {
          id: "job-completion-redelivery-1",
          user_id: "user-1",
          status: "running",
          created: new Date().toISOString(),
          num_failures: 0,
          machine_id: "machine-1",
          command: process.execPath,
          arguments: [childPath],
          environment: {
            KELPIE_TEST_SUCCESS_MARKER: markerPath,
            KELPIE_TEST_INVOCATIONS: invocationPath,
            KELPIE_TEST_EXPENSIVE_WORK: expensiveWorkPath,
          },
          heartbeat_interval: 0.05,
          max_failures: 3,
          container_group_id: "group-1",
          sync: {},
        },
      ]);
      return;
    }

    if (
      req.method === "POST" &&
      requestUrl.pathname ===
        "/jobs/job-completion-redelivery-1/heartbeat"
    ) {
      sendJson(res, 200, { status: "running" });
      return;
    }

    if (
      req.method === "POST" &&
      requestUrl.pathname ===
        "/jobs/job-completion-redelivery-1/completed"
    ) {
      completedCount++;
      if (completedCount === 1) {
        sendJson(res, 503, { error: "simulated control-plane outage" });
      } else {
        sendJson(res, 200, {});
      }
      return;
    }

    if (
      req.method === "POST" &&
      requestUrl.pathname === "/jobs/job-completion-redelivery-1/failed"
    ) {
      failedCount++;
      sendJson(res, 200, {});
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

    sendJson(res, 404, {
      error: `Unhandled ${req.method} ${requestUrl.pathname}`,
    });
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const baseUrl = `http://127.0.0.1:${address.port}`;

  try {
    const stdout = await runWorker({
      runDir,
      baseUrl,
      machineId: "machine-redelivery-test",
      containerGroupId: "group-redelivery-test",
      recreateBetweenJobs: false,
      recreateEveryNJobs: 2,
      timeoutMs: 15_000,
    });
    assert.equal(completedCount, 2);
    assert.equal(failedCount, 0);
    assert.equal(recreateCount, 1);
    assert.equal((stdout.match(/Heartbeat started\./g) ?? []).length, 2);
    assert.equal((stdout.match(/Heartbeat stopped\./g) ?? []).length, 2);
    assert.equal(
      (stdout.match(/"msg":"completion_ack_pending"/g) ?? []).length,
      1
    );
    assert.equal(
      (stdout.match(/"exit_action":"completion_pending"/g) ?? []).length,
      1
    );
    assert.equal(
      (stdout.match(/"exit_action":"completed"/g) ?? []).length,
      1
    );
    assert.equal(
      (await readFile(invocationPath, "utf8")).trim().split("\n").length,
      2
    );
    assert.equal((await readFile(expensiveWorkPath, "utf8")).trim(), "run");
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(runDir, { recursive: true, force: true });
  }
});

test("sync-after distinguishes acknowledgement and artifact failures", async () => {
  const runDir = await mkdtemp(path.join(os.tmpdir(), "kelpie-sync-after-"));
  const syncDir = path.join(runDir, "final-output");
  const missingSyncDir = path.join(runDir, "missing-required-output");
  let workServedCount = 0;
  let completedCount = 0;
  let failedCount = 0;
  let recreateCount = 0;

  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    const requestUrl = new URL(req.url ?? "/", "http://127.0.0.1");

    if (req.method === "GET" && requestUrl.pathname === "/work") {
      if (workServedCount >= 2) {
        sendJson(res, 200, []);
        return;
      }
      const acknowledgementFailure = workServedCount === 0;
      workServedCount++;
      sendJson(res, 200, [
        {
          id: acknowledgementFailure
            ? "job-sync-after-pending-1"
            : "job-sync-after-artifact-failure-1",
          user_id: "user-1",
          status: "running",
          created: new Date().toISOString(),
          num_failures: 0,
          machine_id: "machine-1",
          command: process.execPath,
          arguments: acknowledgementFailure
            ? [
                "-e",
                "require('fs').mkdirSync(process.env.KELPIE_TEST_SYNC_DIR, { recursive: true })",
              ]
            : ["-e", "process.exit(0)"],
          environment: acknowledgementFailure
            ? { KELPIE_TEST_SYNC_DIR: syncDir }
            : {},
          heartbeat_interval: 0.05,
          max_failures: 3,
          container_group_id: "group-1",
          sync: {
            after: [
              {
                bucket: "unused-empty-upload",
                prefix: "unused/",
                local_path: acknowledgementFailure
                  ? syncDir
                  : missingSyncDir,
                direction: "upload",
              },
            ],
          },
        },
      ]);
      return;
    }

    if (
      req.method === "POST" &&
      /\/jobs\/job-sync-after-(pending|artifact-failure)-1\/heartbeat$/.test(
        requestUrl.pathname
      )
    ) {
      sendJson(res, 200, { status: "running" });
      return;
    }

    if (
      req.method === "POST" &&
      requestUrl.pathname === "/jobs/job-sync-after-pending-1/completed"
    ) {
      completedCount++;
      sendJson(res, 503, { error: "simulated control-plane outage" });
      return;
    }

    if (
      req.method === "POST" &&
      /\/jobs\/job-sync-after-(pending|artifact-failure)-1\/failed$/.test(
        requestUrl.pathname
      )
    ) {
      failedCount++;
      sendJson(res, 200, {});
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

    sendJson(res, 404, {
      error: `Unhandled ${req.method} ${requestUrl.pathname}`,
    });
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const baseUrl = `http://127.0.0.1:${address.port}`;

  try {
    const stdout = await runWorker({
      runDir,
      baseUrl,
      machineId: "machine-sync-after-test",
      containerGroupId: "group-sync-after-test",
      recreateBetweenJobs: false,
      recreateEveryNJobs: 2,
    });

    assert.equal(completedCount, 1);
    assert.equal(failedCount, 1);
    assert.equal(recreateCount, 1);
    assert.match(stdout, /"msg":"completion_ack_pending"/);
    assert.match(stdout, /"exit_action":"completion_pending"/);
    assert.equal((stdout.match(/"exit_action":"failed"/g) ?? []).length, 1);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(runDir, { recursive: true, force: true });
  }
});
