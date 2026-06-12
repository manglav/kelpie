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
     write("parent-start.json", { child_pid: child.pid });
     process.on("SIGINT", () => { write("parent-sigint.json"); process.exit(130); });
     process.on("SIGTERM", () => { write("parent-sigterm.json"); process.exit(143); });
     setInterval(() => {}, 1000);
    `
  );

  return parentPath;
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
            arguments: [parentPath],
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
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await rm(runDir, { recursive: true, force: true });
    }
  }
);
