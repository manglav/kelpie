import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

async function waitForFile(filePath: string, timeoutMs = 5000): Promise<string> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      return await readFile(filePath, "utf8");
    } catch (e: any) {
      if (e.code !== "ENOENT") {
        throw e;
      }
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
  }
  throw new Error(`Timed out waiting for ${filePath}`);
}

async function waitForPidGone(pid: number, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      process.kill(pid, 0);
    } catch (e: any) {
      if (e.code === "ESRCH") {
        return;
      }
      throw e;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`PID ${pid} was still alive after ${timeoutMs}ms`);
}

async function writeNestedJobScripts(runDir: string): Promise<string> {
  const grandchildPath = path.join(runDir, "grandchild.js");
  const parentPath = path.join(runDir, "parent.js");

  const common = `
    const fs = require("fs");
    const path = require("path");
    const runDir = process.env.KELPIE_TEST_RUN_DIR;
    function procIds() {
      const stat = fs.readFileSync(\`/proc/\${process.pid}/stat\`, "utf8");
      const fields = stat.slice(stat.lastIndexOf(") ") + 2).trim().split(/\\s+/);
      return { pid: process.pid, ppid: Number(fields[1]), pgid: Number(fields[2]) };
    }
    function write(name, extra = {}) {
      fs.writeFileSync(path.join(runDir, name), JSON.stringify({ ...procIds(), ...extra }));
    }
  `;

  await writeFile(
    grandchildPath,
    `${common}
     write("grandchild-start.json");
     process.on("SIGINT", () => { write("grandchild-sigint.json"); process.exit(130); });
     process.on("SIGTERM", () => { write("grandchild-sigterm.json"); process.exit(143); });
     setInterval(() => {}, 1000);
    `
  );

  await writeFile(
    parentPath,
    `${common}
     const { spawn } = require("child_process");
     const child = spawn(process.execPath, [${JSON.stringify(grandchildPath)}], {
       stdio: "ignore",
       env: process.env,
     });
     write("parent-start.json", {
       child_pid: child.pid,
       kelpie_job_attempt_id: process.env.KELPIE_JOB_ATTEMPT_ID || null,
       screening_job_attempt_id: process.env.SCREENING_JOB_ATTEMPT_ID || null,
     });
     process.on("SIGINT", () => { write("parent-sigint.json"); process.exit(130); });
     process.on("SIGTERM", () => { write("parent-sigterm.json"); process.exit(143); });
     setInterval(() => {}, 1000);
    `
  );

  return parentPath;
}

async function writeQuickExitJobScript(runDir: string): Promise<string> {
  const quickExitPath = path.join(runDir, "quick-exit.js");
  await writeFile(
    quickExitPath,
    `
      const fs = require("fs");
      const path = require("path");
      fs.writeFileSync(path.join(process.env.KELPIE_TEST_RUN_DIR, "quick-done.json"), JSON.stringify({ pid: process.pid }));
    `
  );
  return quickExitPath;
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}

test(
  "worker cancels process group and recreates container after remote cancellation",
  { skip: process.platform !== "linux" },
  async () => {
    const runDir = await mkdtemp(path.join(os.tmpdir(), "kelpie-worker-cancel-"));
    const inputDir = path.join(runDir, "input");
    const outputDir = path.join(runDir, "output");
    const checkpointDir = path.join(runDir, "checkpoint");
    const parentPath = await writeNestedJobScripts(runDir);

    let workServed = false;
    let heartbeatCount = 0;
    let recreateCount = 0;
    let completedCount = 0;
    let failedCount = 0;

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
            id: "job-cancel-1",
            user_id: "user-1",
            status: "running",
            created: new Date().toISOString(),
            num_failures: 0,
            machine_id: "machine-1",
            command: process.execPath,
            arguments: [
              parentPath,
              "--shard-index",
              "7527",
              "--s3-output",
              "s3://docking-results/runs/CHK1/registry-pocket/enamine-real-2026-01-13-6m-raw/outputs/",
              "--work-dir",
              "/app/data/output/work/shard_007527",
            ],
            environment: { KELPIE_TEST_RUN_DIR: runDir },
            heartbeat_interval: 0.05,
            max_failures: 3,
            container_group_id: "group-1",
          },
        ]);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/jobs/job-cancel-1/heartbeat") {
        heartbeatCount++;
        const nestedWorkerStarted = existsSync(path.join(runDir, "grandchild-start.json"));
        sendJson(res, 200, { status: nestedWorkerStarted ? "canceled" : "running" });
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/jobs/job-cancel-1/completed") {
        completedCount++;
        sendJson(res, 200, {});
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/jobs/job-cancel-1/failed") {
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

      sendJson(res, 404, { error: `Unhandled ${req.method} ${requestUrl.pathname}` });
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
          INPUT_DIR: inputDir,
          OUTPUT_DIR: outputDir,
          CHECKPOINT_DIR: checkpointDir,
          KELPIE_STATE_FILE: path.join(runDir, "kelpie-state.json"),
          KELPIE_RECREATE_AFTER_CANCELED_JOB: "true",
          KELPIE_CANCEL_SIGINT_GRACE_S: "1",
          KELPIE_CANCEL_SIGTERM_GRACE_S: "1",
          KELPIE_CANCEL_SIGKILL_GRACE_S: "1",
          KELPIE_CANCEL_PROGRESS_LOG_INTERVAL_S: "0",
          KELPIE_RECREATE_BETWEEN_JOBS: "false",
          KELPIE_RECREATE_EVERY_N_JOBS: "0",
          SCREENING_WORKER_START_ID: "worker-start-test-1",
          SALAD_MACHINE_ID: "machine-test-1",
          SALAD_CONTAINER_GROUP_ID: "group-test-1",
          KELPIE_JOB_HEARTBEAT_INTERVAL_S: "0.05",
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
          reject(new Error(`Kelpie worker timed out. stdout=${stdout} stderr=${stderr}`));
        }, 10000);
        worker.on("exit", (code) => {
          clearTimeout(timer);
          resolve(code);
        });
      });

      const grandchildStart = JSON.parse(
        await waitForFile(path.join(runDir, "grandchild-start.json"))
      ) as { pid: number; pgid: number };
      const parentStart = JSON.parse(
        await waitForFile(path.join(runDir, "parent-start.json"))
      ) as {
        kelpie_job_attempt_id: string;
        screening_job_attempt_id: string;
      };

      assert.equal(exitCode, 0, `stdout=${stdout} stderr=${stderr}`);
      assert.ok(heartbeatCount > 0);
      assert.equal(failedCount, 0);
      assert.equal(completedCount, 0);
      assert.equal(recreateCount, 1);
      await waitForFile(path.join(runDir, "grandchild-sigint.json"));
      await waitForPidGone(grandchildStart.pid);
      assert.match(stdout, /remote_cancellation_observed/);
      assert.match(stdout, /job_process_group_signal_sent/);
      assert.match(stdout, /container_recreate_requested/);
      assert.match(stdout, /kelpie_job_received/);
      assert.match(stdout, /kelpie_job_start/);
      assert.match(stdout, /kelpie_job_exit/);
      assert.match(
        stdout,
        /"run_name":"CHK1\/registry-pocket\/enamine-real-2026-01-13-6m-raw"/
      );
      assert.match(stdout, /"shard":7527/);
      assert.match(stdout, /"screening_worker_start_id":"worker-start-test-1"/);
      assert.match(stdout, /"kelpie_job_attempt_id":"[0-9a-f-]{36}"/);
      assert.match(parentStart.kelpie_job_attempt_id, /^[0-9a-f-]{36}$/);
      assert.equal(
        parentStart.screening_job_attempt_id,
        parentStart.kelpie_job_attempt_id
      );
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await rm(runDir, { recursive: true, force: true });
    }
  }
);

test(
  "worker treats cancellation after local process exit as a late benign race",
  { skip: process.platform !== "linux" },
  async () => {
    const runDir = await mkdtemp(path.join(os.tmpdir(), "kelpie-worker-late-cancel-"));
    const inputDir = path.join(runDir, "input");
    const outputDir = path.join(runDir, "output");
    const checkpointDir = path.join(runDir, "checkpoint");
    const quickExitPath = await writeQuickExitJobScript(runDir);

    let workServed = false;
    let heartbeatCount = 0;
    let recreateCount = 0;
    let completedCount = 0;
    let failedCount = 0;

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
            id: "job-late-cancel-1",
            user_id: "user-1",
            status: "running",
            created: new Date().toISOString(),
            num_failures: 0,
            machine_id: "machine-1",
            command: process.execPath,
            arguments: [
              quickExitPath,
              "--shard-index",
              "448",
              "--s3-output",
              "s3://docking-results/runs/CHK1/registry-pocket/enamine-real-2026-01-top13-6m-aibind/outputs/",
            ],
            environment: { KELPIE_TEST_RUN_DIR: runDir },
            heartbeat_interval: 0.05,
            max_failures: 3,
            container_group_id: "group-1",
            sync: {},
          },
        ]);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/jobs/job-late-cancel-1/heartbeat") {
        heartbeatCount++;
        const localProcessExited = existsSync(path.join(runDir, "quick-done.json"));
        sendJson(res, 200, { status: localProcessExited ? "canceled" : "running" });
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/jobs/job-late-cancel-1/completed") {
        completedCount++;
        sendJson(res, 200, {});
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/jobs/job-late-cancel-1/failed") {
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

      sendJson(res, 404, { error: `Unhandled ${req.method} ${requestUrl.pathname}` });
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
          INPUT_DIR: inputDir,
          OUTPUT_DIR: outputDir,
          CHECKPOINT_DIR: checkpointDir,
          KELPIE_STATE_FILE: path.join(runDir, "kelpie-state.json"),
          KELPIE_RECREATE_AFTER_CANCELED_JOB: "true",
          KELPIE_CANCEL_PROGRESS_LOG_INTERVAL_S: "0.05",
          KELPIE_RECREATE_BETWEEN_JOBS: "true",
          KELPIE_RECREATE_EVERY_N_JOBS: "0",
          SCREENING_WORKER_START_ID: "worker-start-late-cancel-test",
          SALAD_MACHINE_ID: "machine-test-late-cancel",
          SALAD_CONTAINER_GROUP_ID: "group-test-late-cancel",
          KELPIE_JOB_HEARTBEAT_INTERVAL_S: "0.05",
          HEARTBEAT_INTERVAL_S: "1",
          MAX_TIME_WITH_NO_WORK_S: "1",
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
          reject(new Error(`Kelpie worker timed out. stdout=${stdout} stderr=${stderr}`));
        }, 10000);
        worker.on("exit", (code) => {
          clearTimeout(timer);
          resolve(code);
        });
      });

      await waitForFile(path.join(runDir, "quick-done.json"));
      assert.equal(exitCode, 0, `stdout=${stdout} stderr=${stderr}`);
      assert.ok(heartbeatCount > 0);
      assert.equal(completedCount, 1);
      assert.equal(failedCount, 0);
      assert.equal(recreateCount, 1);
      assert.match(stdout, /remote_cancellation_observed/);
      assert.match(stdout, /late_remote_cancellation_after_process_exit/);
      assert.match(stdout, /"cancel_signal_target":"none"/);
      assert.match(stdout, /"exit_action":"completed"/);
      assert.match(stdout, /"late_cancellation_after_process_exit":true/);
      assert.match(stdout, /"recreate_reason":"between_jobs"/);
      assert.doesNotMatch(stdout, /Remote cancellation still waiting for process exit/);
      assert.doesNotMatch(stdout, /"recreate_reason":"canceled_job"/);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await rm(runDir, { recursive: true, force: true });
    }
  }
);
