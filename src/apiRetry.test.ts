import assert from "node:assert/strict";
import { createServer, Server } from "node:http";
import test from "node:test";

process.env.KELPIE_API_KEY = "test-only-key";
process.env.KELPIE_API_REQUEST_TIMEOUT_S = "0.03";
process.env.KELPIE_API_RETRY_INITIAL_DELAY_S = "0.2";
process.env.KELPIE_API_RETRY_MAX_DELAY_S = "0.2";

const apiModule = import("./api");

async function listen(server: Server): Promise<string> {
  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  assert(address && typeof address !== "string");
  return `http://127.0.0.1:${address.port}`;
}

async function close(server: Server): Promise<void> {
  server.closeAllConnections();
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

test("times out each attempt and retries up to the attempt limit", async () => {
  let requests = 0;
  const server = createServer(() => {
    requests++;
    // Deliberately leave the response open so the per-attempt timeout fires.
  });
  const baseUrl = await listen(server);

  try {
    const { fetchUpToNTimes } = await apiModule;
    await assert.rejects(
      fetchUpToNTimes(`${baseUrl}/timeout`, { method: "GET" }, 2),
      /Failed to fetch data/
    );
    assert.equal(requests, 2);
  } finally {
    await close(server);
  }
});

test("does not sleep after the final failed attempt", async () => {
  const server = createServer((_request, response) => {
    response.writeHead(503, { "Content-Type": "text/plain" });
    response.end("unavailable");
  });
  const baseUrl = await listen(server);

  try {
    const { fetchUpToNTimes } = await apiModule;
    const startedAt = Date.now();
    await assert.rejects(
      fetchUpToNTimes(`${baseUrl}/unavailable`, { method: "GET" }, 1),
      /Failed to fetch data/
    );
    assert(
      Date.now() - startedAt < 150,
      "a single final failure should not incur the configured 200ms delay"
    );
  } finally {
    await close(server);
  }
});

test("an explicit lifecycle abort stops without making an attempt", async () => {
  let requests = 0;
  const server = createServer((_request, response) => {
    requests++;
    response.end("unexpected");
  });
  const baseUrl = await listen(server);
  const controller = new AbortController();
  controller.abort(new Error("worker stopping"));

  try {
    const { fetchUpToNTimes } = await apiModule;
    await assert.rejects(
      fetchUpToNTimes(
        `${baseUrl}/aborted`,
        { method: "GET", signal: controller.signal },
        3
      ),
      /worker stopping/
    );
    assert.equal(requests, 0);
  } finally {
    await close(server);
  }
});
