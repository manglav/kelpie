import { Logger } from "pino";

import { TaskStatus } from "./types";

type HeartbeatResponse = { status: TaskStatus };

export type JobHeartbeatOptions = {
  intervalMs: number;
  sendHeartbeat: (signal: AbortSignal) => Promise<HeartbeatResponse>;
  onHeartbeatAccepted: (numHeartbeats: number) => Promise<void>;
  onCanceled: () => Promise<void>;
  log: Logger;
};

enum JobHeartbeatState {
  Idle = "idle",
  Running = "running",
  Stopping = "stopping",
  Stopped = "stopped",
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** Owns the single heartbeat loop for one job attempt. */
export class JobHeartbeat {
  private state = JobHeartbeatState.Idle;
  private loopPromise: Promise<void> | null = null;
  private wakeInterval: (() => void) | null = null;
  private numHeartbeats = 0;
  private readonly abortController = new AbortController();

  constructor(private readonly options: JobHeartbeatOptions) {}

  start(): void {
    if (this.state !== JobHeartbeatState.Idle) {
      throw new Error(`Cannot start heartbeat from ${this.state} state`);
    }

    this.state = JobHeartbeatState.Running;
    this.loopPromise = this.run();
  }

  async stop(): Promise<void> {
    if (this.state === JobHeartbeatState.Idle) {
      this.state = JobHeartbeatState.Stopped;
      return;
    }

    if (this.state === JobHeartbeatState.Running) {
      this.state = JobHeartbeatState.Stopping;
      this.abortController.abort();
      this.interruptInterval();
    }

    await this.loopPromise;
  }

  private isRunning(): boolean {
    return this.state === JobHeartbeatState.Running;
  }

  private async run(): Promise<void> {
    this.options.log.info("Heartbeat started.");

    try {
      while (this.isRunning()) {
        let response: HeartbeatResponse;
        try {
          response = await this.options.sendHeartbeat(
            this.abortController.signal
          );
        } catch (error: unknown) {
          if (this.isRunning()) {
            this.options.log.error(
              `Heartbeat request failed; retrying while job is running: ${errorMessage(error)}`
            );
            await this.waitForInterval();
          }
          continue;
        }

        if (!this.isRunning()) {
          break;
        }

        this.numHeartbeats++;
        if (response.status === "canceled") {
          this.state = JobHeartbeatState.Stopping;
          this.options.log.info("Job was canceled, stopping heartbeat.");
          try {
            await this.options.onCanceled();
          } catch (error: unknown) {
            this.options.log.error(
              `Heartbeat cancellation handler failed: ${errorMessage(error)}`
            );
          }
          break;
        }

        try {
          await this.options.onHeartbeatAccepted(this.numHeartbeats);
        } catch (error: unknown) {
          this.options.log.error(
            `Heartbeat post-processing failed: ${errorMessage(error)}`
          );
        }

        if (this.isRunning()) {
          await this.waitForInterval();
        }
      }
    } finally {
      this.interruptInterval();
      this.state = JobHeartbeatState.Stopped;
      this.options.log.info("Heartbeat stopped.");
    }
  }

  private waitForInterval(): Promise<void> {
    if (!this.isRunning()) {
      return Promise.resolve();
    }

    return new Promise((resolve) => {
      let timer: NodeJS.Timeout | null = null;
      const finish = () => {
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
        if (this.wakeInterval === finish) {
          this.wakeInterval = null;
        }
        resolve();
      };

      this.wakeInterval = finish;
      timer = setTimeout(finish, this.options.intervalMs);
    });
  }

  private interruptInterval(): void {
    this.wakeInterval?.();
  }
}
