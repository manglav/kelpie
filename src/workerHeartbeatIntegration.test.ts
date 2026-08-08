import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}

async function waitFor(
  condition: () => boolean,
  timeoutMs = 5000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() >= deadline) {
      throw new Error("Timed out waiting for worker heartbeat test condition");
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

test("late heartbeat failure cannot restart after the child exits", async () => {
  const runDir = await mkdtemp(path.join(os.tmpdir(), "kelpie-heartbeat-"));
  const inputDir = path.join(runDir, "input");
  const outputDir = path.join(runDir, "output");
  const checkpointDir = path.join(runDir, "checkpoint");
  const childPath = path.join(runDir, "quick-exit.js");
  await writeFile(
    childPath,
    `
      const fs = require("fs");
      const path = require("path");
      fs.writeFileSync(path.join(process.env.KELPIE_TEST_RUN_DIR, "child-finished"), "done");
    `
  );

  let workServed = false;
  let heartbeatCount = 0;
  let postCompletionHeartbeats = 0;
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
          id: "job-heartbeat-race-1",
          user_id: "user-1",
          status: "running",
          created: new Date().toISOString(),
          num_failures: 0,
          machine_id: "machine-1",
          command: process.execPath,
          arguments: [childPath],
          environment: { KELPIE_TEST_RUN_DIR: runDir },
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
      requestUrl.pathname === "/jobs/job-heartbeat-race-1/heartbeat"
    ) {
      heartbeatCount++;
      if (completedCount > 0) {
        postCompletionHeartbeats++;
      }

      if (heartbeatCount === 1) {
        void (async () => {
          await waitFor(() => existsSync(path.join(runDir, "child-finished")));
          await new Promise((resolve) => setTimeout(resolve, 1100));
          sendJson(res, 500, { error: "simulated late heartbeat failure" });
        })();
        return;
      }

      sendJson(res, 200, { status: "running" });
      return;
    }

    if (
      req.method === "POST" &&
      requestUrl.pathname === "/jobs/job-heartbeat-race-1/completed"
    ) {
      completedCount++;
      sendJson(res, 200, {});
      return;
    }

    if (
      req.method === "POST" &&
      requestUrl.pathname === "/jobs/job-heartbeat-race-1/failed"
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
        KELPIE_RECREATE_BETWEEN_JOBS: "true",
        KELPIE_RECREATE_EVERY_N_JOBS: "0",
        SALAD_MACHINE_ID: "machine-heartbeat-test",
        SALAD_CONTAINER_GROUP_ID: "group-heartbeat-test",
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
      }, 10_000);
      worker.on("exit", (code) => {
        clearTimeout(timer);
        resolve(code);
      });
    });

    assert.equal(exitCode, 0, `stdout=${stdout} stderr=${stderr}`);
    assert.equal(heartbeatCount, 1);
    assert.equal(postCompletionHeartbeats, 0);
    assert.equal(completedCount, 1);
    assert.equal(failedCount, 0);
    assert.equal(recreateCount, 1);
    assert.equal((stdout.match(/Heartbeat started\./g) ?? []).length, 1);
    assert.equal((stdout.match(/Heartbeat stopped\./g) ?? []).length, 1);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(runDir, { recursive: true, force: true });
  }
});
