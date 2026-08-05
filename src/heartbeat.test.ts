import assert from "node:assert/strict";
import test from "node:test";

import pino from "pino";

import { JobHeartbeat, JobHeartbeatOptions } from "./heartbeat";
import { TaskStatus } from "./types";

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: Error) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function waitFor(
  condition: () => boolean,
  timeoutMs = 1000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() >= deadline) {
      throw new Error("Timed out waiting for heartbeat test condition");
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

function options(
  overrides: Partial<JobHeartbeatOptions> = {}
): JobHeartbeatOptions {
  return {
    intervalMs: 60_000,
    sendHeartbeat: async () => ({ status: "running" }),
    onHeartbeatAccepted: async () => {},
    onCanceled: async () => {},
    log: pino({ enabled: false }),
    ...overrides,
  };
}

test("transient request failure recovers in the same heartbeat loop", async () => {
  let requests = 0;
  let cancellations = 0;
  const heartbeat = new JobHeartbeat(
    options({
      intervalMs: 0,
      sendHeartbeat: async () => {
        requests++;
        if (requests === 1) {
          throw new Error("temporary API failure");
        }
        return { status: "canceled" };
      },
      onCanceled: async () => {
        cancellations++;
      },
    })
  );

  heartbeat.start();
  await waitFor(() => cancellations === 1);
  await heartbeat.stop();

  assert.equal(requests, 2);
  assert.equal(cancellations, 1);
});

test("terminal stop aborts an in-flight request and prevents another", async () => {
  const request = deferred<{ status: TaskStatus }>();
  let requests = 0;
  let signalWasAborted = false;
  const heartbeat = new JobHeartbeat(
    options({
      intervalMs: 0,
      sendHeartbeat: async (signal) => {
        requests++;
        signal.addEventListener(
          "abort",
          () => {
            signalWasAborted = signal.aborted;
            request.reject(new Error("heartbeat stopped"));
          },
          { once: true }
        );
        return request.promise;
      },
    })
  );

  heartbeat.start();
  await waitFor(() => requests === 1);

  await heartbeat.stop();
  await new Promise((resolve) => setTimeout(resolve, 5));

  assert.equal(requests, 1);
  assert.equal(signalWasAborted, true);
});

test("stop interrupts interval sleep and is idempotent", async () => {
  let requests = 0;
  let accepted = 0;
  const heartbeat = new JobHeartbeat(
    options({
      sendHeartbeat: async () => {
        requests++;
        return { status: "running" };
      },
      onHeartbeatAccepted: async () => {
        accepted++;
      },
    })
  );

  heartbeat.start();
  await waitFor(() => accepted === 1);

  await Promise.all([heartbeat.stop(), heartbeat.stop()]);
  await new Promise((resolve) => setTimeout(resolve, 5));

  assert.equal(requests, 1);
  assert.throws(() => heartbeat.start(), /Cannot start heartbeat from stopped state/);
});

test("remote cancellation is terminal and invokes its callback once", async () => {
  let requests = 0;
  let cancellations = 0;
  const heartbeat = new JobHeartbeat(
    options({
      sendHeartbeat: async () => {
        requests++;
        return { status: "canceled" };
      },
      onCanceled: async () => {
        cancellations++;
      },
    })
  );

  heartbeat.start();
  await waitFor(() => cancellations === 1);
  await heartbeat.stop();

  assert.equal(requests, 1);
  assert.equal(cancellations, 1);
});
