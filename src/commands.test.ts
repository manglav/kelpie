import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { CommandExecutor, StopSignalStep } from "./commands";

async function waitForFile(filePath: string, timeoutMs = 3000): Promise<string> {
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

async function waitForJson<T>(filePath: string, timeoutMs = 3000): Promise<T> {
  return JSON.parse(await waitForFile(filePath, timeoutMs)) as T;
}

async function writeProbeScripts(runDir: string, behavior: "exit-on-sigint" | "ignore-until-sigkill") {
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

  const signalHandlers =
    behavior === "exit-on-sigint"
      ? `
        process.on("SIGINT", () => { write("grandchild-sigint.json"); process.exit(130); });
        process.on("SIGTERM", () => { write("grandchild-sigterm.json"); process.exit(143); });
      `
      : `
        process.on("SIGINT", () => write("grandchild-sigint.json"));
        process.on("SIGTERM", () => write("grandchild-sigterm.json"));
      `;

  await writeFile(
    grandchildPath,
    `${common}
     write("grandchild-start.json");
     ${signalHandlers}
     setInterval(() => {}, 1000);
    `
  );

  const parentHandlers =
    behavior === "exit-on-sigint"
      ? `
        process.on("SIGINT", () => { write("parent-sigint.json"); process.exit(130); });
        process.on("SIGTERM", () => { write("parent-sigterm.json"); process.exit(143); });
      `
      : `
        process.on("SIGINT", () => write("parent-sigint.json"));
        process.on("SIGTERM", () => write("parent-sigterm.json"));
      `;

  await writeFile(
    parentPath,
    `${common}
     const { spawn } = require("child_process");
     const child = spawn(process.execPath, [${JSON.stringify(grandchildPath)}], {
       stdio: "ignore",
       env: process.env,
     });
     write("parent-start.json", { child_pid: child.pid });
     ${parentHandlers}
     setInterval(() => {}, 1000);
    `
  );

  return { parentPath, grandchildPath };
}

test(
  "execute launches a job process group and cancellation reaches grandchildren",
  { skip: process.platform !== "linux" },
  async () => {
    const runDir = await mkdtemp(path.join(os.tmpdir(), "kelpie-pgid-"));
    try {
      const { parentPath } = await writeProbeScripts(runDir, "exit-on-sigint");
      const executor = new CommandExecutor();
      const executeResult = executor
        .execute(process.execPath, [parentPath], {
          KELPIE_TEST_RUN_DIR: runDir,
        })
        .then((code) => code)
        .catch((error: Error) => error);

      const parentStart = await waitForJson<{ pid: number; pgid: number }>(
        path.join(runDir, "parent-start.json")
      );
      const grandchildStart = await waitForJson<{ pid: number; pgid: number }>(
        path.join(runDir, "grandchild-start.json")
      );

      assert.equal(parentStart.pgid, parentStart.pid);
      assert.equal(grandchildStart.pgid, parentStart.pgid);

      const cleanup = await executor.stopProcessGroup({
        reason: "test_remote_cancel",
        sequence: [{ signal: "SIGINT", graceMs: 2000 }],
      });
      const exitCode = await executeResult;

      assert.equal(exitCode, 130);
      assert.equal(cleanup.groupEmpty, true);
      assert.equal(cleanup.finalSignal, "SIGINT");
      await waitForFile(path.join(runDir, "grandchild-sigint.json"));
    } finally {
      await rm(runDir, { recursive: true, force: true });
    }
  }
);

test(
  "process group cleanup escalates to SIGKILL when earlier signals are ignored",
  { skip: process.platform !== "linux" },
  async () => {
    const runDir = await mkdtemp(path.join(os.tmpdir(), "kelpie-pgid-"));
    try {
      const { parentPath } = await writeProbeScripts(
        runDir,
        "ignore-until-sigkill"
      );
      const executor = new CommandExecutor();
      const executeResult = executor
        .execute(process.execPath, [parentPath], {
          KELPIE_TEST_RUN_DIR: runDir,
        })
        .then((code) => code)
        .catch((error: Error) => error);

      await waitForFile(path.join(runDir, "grandchild-start.json"));

      const sequence: StopSignalStep[] = [
        { signal: "SIGINT", graceMs: 100 },
        { signal: "SIGTERM", graceMs: 100 },
        { signal: "SIGKILL", graceMs: 2000 },
      ];
      const cleanup = await executor.stopProcessGroup({
        reason: "test_remote_cancel",
        sequence,
      });

      const outcome = await executeResult;
      assert.ok(outcome instanceof Error);
      assert.match(outcome.message, /SIGKILL/);
      assert.equal(cleanup.groupEmpty, true);
      assert.equal(cleanup.finalSignal, "SIGKILL");
      assert.deepEqual(
        cleanup.signalAttempts.map((attempt) => attempt.signal),
        ["SIGINT", "SIGTERM", "SIGKILL"]
      );
      await waitForFile(path.join(runDir, "grandchild-sigint.json"));
      await waitForFile(path.join(runDir, "grandchild-sigterm.json"));
    } finally {
      await rm(runDir, { recursive: true, force: true });
    }
  }
);

test(
  "process group cleanup records incomplete cleanup when the group is still alive",
  { skip: process.platform !== "linux" },
  async () => {
    const runDir = await mkdtemp(path.join(os.tmpdir(), "kelpie-pgid-"));
    try {
      const { parentPath } = await writeProbeScripts(
        runDir,
        "ignore-until-sigkill"
      );
      const executor = new CommandExecutor();
      const executeResult = executor
        .execute(process.execPath, [parentPath], {
          KELPIE_TEST_RUN_DIR: runDir,
        })
        .then((code) => code)
        .catch((error: Error) => error);

      await waitForFile(path.join(runDir, "grandchild-start.json"));

      const incomplete = await executor.stopProcessGroup({
        reason: "test_short_cleanup",
        sequence: [{ signal: "SIGINT", graceMs: 50 }],
      });
      assert.equal(incomplete.groupEmpty, false);
      assert.equal(incomplete.finalSignal, "SIGINT");

      const forced = await executor.stopProcessGroup({
        reason: "test_force_cleanup",
        sequence: [{ signal: "SIGKILL", graceMs: 2000 }],
      });
      const outcome = await executeResult;
      assert.ok(outcome instanceof Error);
      assert.match(outcome.message, /SIGKILL/);
      assert.equal(forced.groupEmpty, true);
    } finally {
      await rm(runDir, { recursive: true, force: true });
    }
  }
);
