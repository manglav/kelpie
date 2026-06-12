import { spawn, ChildProcess } from "child_process";
import { log } from "./logger";
import { Logger } from "pino";

export type RunningJob = {
  child: ChildProcess;
  pid: number;
  pgid: number;
};

export type StopSignalStep = {
  signal: NodeJS.Signals;
  graceMs: number;
};

export type InterruptResult =
  | {
      sent: true;
      signal: NodeJS.Signals;
      target: "process_group";
      pid: number;
      pgid: number;
    }
  | {
      sent: false;
      signal: NodeJS.Signals;
      target: "process_group";
      pid: number;
      pgid: number;
      error: string;
    }
  | {
      sent: false;
      signal: NodeJS.Signals;
      target: "none";
      reason: "no_process";
    };

export type ProcessGroupSignalAttempt = {
  signal: NodeJS.Signals;
  signalSent: boolean;
  graceMs: number;
  groupEmpty: boolean;
  waitElapsedMs: number;
  error?: string;
};

export type ProcessGroupCleanupResult = {
  reason: string;
  pid?: number;
  pgid?: number;
  groupEmpty: boolean;
  finalSignal: NodeJS.Signals | null;
  cleanupElapsedMs: number;
  signalAttempts: ProcessGroupSignalAttempt[];
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class CommandExecutor {
  private running: RunningJob | null = null;

  getRunningJob(): RunningJob | null {
    return this.running;
  }

  clearRunningJob(): void {
    this.running = null;
  }

  private processGroupExists(pgid: number): boolean {
    try {
      process.kill(-pgid, 0);
      return true;
    } catch (e: any) {
      if (e.code === "ESRCH") {
        return false;
      }
      if (e.code === "EPERM") {
        return true;
      }
      throw e;
    }
  }

  private async waitForProcessGroupExit(
    pgid: number,
    timeoutMs: number
  ): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    while (this.processGroupExists(pgid)) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        return false;
      }
      await sleep(Math.min(50, remaining));
    }
    return true;
  }

  private signalProcessGroup(
    running: RunningJob,
    signal: NodeJS.Signals,
    logger: Logger
  ): InterruptResult {
    try {
      process.kill(-running.pgid, signal);
      logger.info(
        {
          signal,
          target: "process_group",
          pid: running.pid,
          pgid: running.pgid,
          sent: true,
        },
        "job_process_group_signal_sent"
      );
      return {
        sent: true,
        signal,
        target: "process_group",
        pid: running.pid,
        pgid: running.pgid,
      };
    } catch (e: any) {
      const expectedGone = e.code === "ESRCH";
      logger[expectedGone ? "info" : "error"](
        {
          signal,
          target: "process_group",
          pid: running.pid,
          pgid: running.pgid,
          sent: false,
          error: e.message,
        },
        "job_process_group_signal_sent"
      );
      if (!expectedGone) {
        throw e;
      }
      return {
        sent: false,
        signal,
        target: "process_group",
        pid: running.pid,
        pgid: running.pgid,
        error: e.message,
      };
    }
  }

  /**
   * Executes a command with arguments and custom environment variables.
   * Returns a promise that resolves with the exit code of the subprocess.
   * @param command The command to execute.
   * @param args An array of arguments for the command.
   * @param envAdditions Object with additional environment variables.
   */
  async execute(
    command: string,
    args: string[],
    envAdditions: NodeJS.ProcessEnv,
    logger: Logger = log
  ): Promise<number | null> {
    const env = { ...process.env, ...envAdditions }; // Merge parent environment with additions

    return new Promise((resolve, reject) => {
      const child = spawn(command, args, {
        env: env,
        stdio: "inherit", // Use the parent's stdin, stdout, and stderr
        detached: true,
      });

      if (child.pid === undefined) {
        reject(new Error("Spawned subprocess has no pid"));
        return;
      }

      const running: RunningJob = {
        child,
        pid: child.pid,
        pgid: child.pid,
      };
      this.running = running;
      logger.info(
        { pid: running.pid, pgid: running.pgid },
        "job_process_group_started"
      );

      child.on("error", (err) => {
        logger.error(`Failed to start subprocess: ${err.message}`);
        reject(err);
      });

      child.on("exit", (code, signal) => {
        logger.info(`Process exited with code ${code}, signal ${signal}`);
        if (code !== null) {
          resolve(code);
        } else {
          reject(new Error(`Process terminated due to signal: ${signal}`));
        }
      });
    });
  }

  /**
   * Interrupts the currently running subprocess.
   */
  interrupt(signal: NodeJS.Signals = "SIGINT"): InterruptResult {
    if (!this.running) {
      log.info("No process to interrupt");
      return { sent: false, signal, target: "none", reason: "no_process" };
    }
    return this.signalProcessGroup(this.running, signal, log);
  }

  async stopProcessGroup({
    reason,
    sequence,
    logger = log,
  }: {
    reason: string;
    sequence: StopSignalStep[];
    logger?: Logger;
  }): Promise<ProcessGroupCleanupResult> {
    const startedAt = Date.now();
    const running = this.running;
    if (!running) {
      const result: ProcessGroupCleanupResult = {
        reason,
        groupEmpty: true,
        finalSignal: null,
        cleanupElapsedMs: 0,
        signalAttempts: [],
      };
      logger.info({ reason, group_empty: true }, "job_process_group_cleanup_complete");
      return result;
    }

    logger.warn(
      { reason, pid: running.pid, pgid: running.pgid },
      "job_process_group_cleanup_start"
    );

    let groupEmpty = !this.processGroupExists(running.pgid);
    let finalSignal: NodeJS.Signals | null = null;
    const signalAttempts: ProcessGroupSignalAttempt[] = [];

    for (const step of sequence) {
      if (groupEmpty) {
        break;
      }

      let signalSent = false;
      let error: string | undefined;
      try {
        const signalResult = this.signalProcessGroup(
          running,
          step.signal,
          logger
        );
        signalSent = signalResult.sent;
        if (!signalResult.sent && signalResult.target === "process_group") {
          error = signalResult.error;
        }
      } catch (e: any) {
        error = e.message;
        throw e;
      }

      finalSignal = step.signal;
      const waitStartedAt = Date.now();
      groupEmpty = await this.waitForProcessGroupExit(
        running.pgid,
        step.graceMs
      );
      const waitElapsedMs = Date.now() - waitStartedAt;
      const attempt: ProcessGroupSignalAttempt = {
        signal: step.signal,
        signalSent,
        graceMs: step.graceMs,
        groupEmpty,
        waitElapsedMs,
        ...(error ? { error } : {}),
      };
      signalAttempts.push(attempt);
      logger.warn(
        {
          reason,
          pid: running.pid,
          pgid: running.pgid,
          signal: step.signal,
          signal_sent: signalSent,
          grace_ms: step.graceMs,
          group_empty: groupEmpty,
          wait_elapsed_ms: waitElapsedMs,
          ...(error ? { error } : {}),
        },
        "job_process_group_signal_wait"
      );
    }

    groupEmpty = !this.processGroupExists(running.pgid);
    const cleanupElapsedMs = Date.now() - startedAt;
    const result: ProcessGroupCleanupResult = {
      reason,
      pid: running.pid,
      pgid: running.pgid,
      groupEmpty,
      finalSignal,
      cleanupElapsedMs,
      signalAttempts,
    };

    logger.warn(
      {
        reason,
        pid: running.pid,
        pgid: running.pgid,
        group_empty: groupEmpty,
        final_signal: finalSignal,
        cleanup_elapsed_ms: cleanupElapsedMs,
      },
      "job_process_group_cleanup_complete"
    );

    if (groupEmpty && this.running?.pid === running.pid) {
      this.running = null;
    }

    return result;
  }
}
