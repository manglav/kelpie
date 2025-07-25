import assert from "assert";
import { log as baseLogger } from "./logger";
import { Logger } from "pino";
import { Task } from "./types";
import { SaladCloudImdsSdk } from "@saladtechnologies-oss/salad-cloud-imds-sdk";
import state from "./state";

let {
  KELPIE_API_URL = "https://kelpie.saladexamples.com",
  KELPIE_API_KEY,
  SALAD_API_KEY,
  SALAD_PROJECT,
  SALAD_ORGANIZATION,
  SALAD_MACHINE_ID = "",
  SALAD_CONTAINER_GROUP_ID = "",
  MAX_RETRIES = "3",
  MAX_JOB_FAILURES = "5",
} = process.env;

assert(KELPIE_API_URL, "KELPIE_API_URL is required");

if (KELPIE_API_URL.endsWith("/")) {
  KELPIE_API_URL = KELPIE_API_URL.slice(0, -1);
}

if (SALAD_API_KEY && !SALAD_PROJECT) {
  throw new Error(
    "SALAD_API_KEY is set but SALAD_PROJECT is not. Please set SALAD_PROJECT to the name of your Salad project."
  );
}

if (!KELPIE_API_KEY && !SALAD_API_KEY && !SALAD_PROJECT) {
  throw new Error(
    "JWT Authentication requested, but no SALAD_PROJECT is defined. Please set SALAD_PROJECT to the name of your Salad project."
  );
}

const maxRetries = parseInt(MAX_RETRIES, 10);
const maxJobFailures = parseInt(MAX_JOB_FAILURES, 10);
const imdsUrl = "http://169.254.169.254";

const headers: Record<string, string> = {
  "Content-Type": "application/json",
};
if (KELPIE_API_KEY) {
  headers["X-Kelpie-Key"] = KELPIE_API_KEY;
} else if (SALAD_API_KEY) {
  headers["Salad-Api-Key"] = SALAD_API_KEY;
}
let saladOrgName = SALAD_ORGANIZATION;

/**
 * Get the headers to be used for API requests.
 *
 * If KELPIE_API_KEY is set, it will return the headers with the KELPIE_API_KEY.
 * If SALAD_API_KEY is set, it will return the headers with the Salad API key, organization name, and project.
 * If neither is set, it will fetch a JWT token from the Salad IMDS and return the headers with the JWT token, and project.
 *
 * @returns A promise that resolves to the headers to be used for API requests.
 */
async function getHeaders(): Promise<Record<string, string>> {
  if (KELPIE_API_KEY) {
    return headers;
  }
  let saladJWT: string | undefined;
  if (!saladOrgName) {
    const {
      token,
      payload: { salad_organization_name },
    } = await getSaladJWT(baseLogger);
    saladOrgName = salad_organization_name as string;
    saladJWT = token;
  }
  if (SALAD_API_KEY) {
    headers["Salad-Organization"] = saladOrgName;
    headers["Salad-Project"] = SALAD_PROJECT!;
    return headers;
  }

  if (!saladJWT) {
    const { token } = await getSaladJWT(baseLogger);
    saladJWT = token;
  }
  headers["Authorization"] = `Bearer ${saladJWT}`;
  headers["Salad-Project"] = SALAD_PROJECT!;

  return headers;
}

const imds = new SaladCloudImdsSdk({});

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchUpToNTimes<T>(
  url: string,
  params: any,
  n: number,
  log: Logger = baseLogger
): Promise<T> {
  let retries = 0;
  while (retries < n) {
    try {
      const response = await fetch(url, params);
      if (response.ok) {
        return response.json() as Promise<T>;
      } else {
        const body = await response.text();
        log.warn(`Error fetching data, retrying: ${body}`);
        retries++;
        await sleep(retries * 1000);
        continue;
      }
    } catch (err: any) {
      log.warn(`Error fetching data, retrying: ${err.message}`);
      retries++;
      await sleep(retries * 1000);
      continue;
    }
  }
  throw new Error(`Failed to fetch data: ${url}`);
}

export async function getWork(): Promise<Task | null> {
  const query = new URLSearchParams({
    machine_id: SALAD_MACHINE_ID,
    container_group_id: SALAD_CONTAINER_GROUP_ID,
  }).toString();
  const work = await fetchUpToNTimes<Task[]>(
    `${KELPIE_API_URL}/work?${query}`,
    {
      method: "GET",
      headers: await getHeaders(),
    },
    maxRetries
  );
  if (work.length) {
    return work[0];
  }
  return null;
}

export async function sendHeartbeat(
  jobId: string,
  log: Logger
): Promise<{ status: Task["status"] }> {
  log.debug(`Sending heartbeat`);
  const { status } = await fetchUpToNTimes<{ status: Task["status"] }>(
    `${KELPIE_API_URL}/jobs/${jobId}/heartbeat`,
    {
      method: "POST",
      headers: await getHeaders(),
      body: JSON.stringify({
        machine_id: SALAD_MACHINE_ID,
        container_group_id: SALAD_CONTAINER_GROUP_ID,
      }),
    },
    maxRetries,
    log
  );
  return { status };
}

let numFailures = 0;
export async function reportFailed(jobId: string, log: Logger): Promise<void> {
  log.info(`Reporting job failed`);
  state.finishJob(jobId, "failed", log);
  await fetchUpToNTimes(
    `${KELPIE_API_URL}/jobs/${jobId}/failed`,
    {
      method: "POST",
      headers: await getHeaders(),
      body: JSON.stringify({
        machine_id: SALAD_MACHINE_ID,
        container_group_id: SALAD_CONTAINER_GROUP_ID,
      }),
    },
    maxRetries,
    log
  );
  numFailures++;
  if (numFailures >= maxJobFailures) {
    await reallocateMe("Kelpie: Max Job Failures Exceeded", log);
  }
}

export async function reallocateMe(reason: string, log: Logger): Promise<void> {
  try {
    log.info("Reallocating container via IMDS");
    await imds.metadata.reallocate({
      reason,
    });
  } catch (e: any) {
    log.error(`Failed to reallocate container via IMDS: ${e.message}`);
    log.error("Exiting process");
    process.exit(1);
  }
}

export async function reportCompleted(
  jobId: string,
  log: Logger
): Promise<void> {
  log.info("Reporting job completed");
  state.finishJob(jobId, "completed", log);
  await fetchUpToNTimes(
    `${KELPIE_API_URL}/jobs/${jobId}/completed`,
    {
      method: "POST",
      headers: await getHeaders(),
      body: JSON.stringify({
        machine_id: SALAD_MACHINE_ID,
        container_group_id: SALAD_CONTAINER_GROUP_ID,
      }),
    },
    maxRetries,
    log
  );
}

export class HeartbeatManager {
  private active: boolean = false;
  private jobId: string;
  private waiter: Promise<void> | null = null;
  private log: Logger;
  private numHeartbeats: number = 0;

  constructor(jobId: string, log: Logger) {
    this.log = log;
    this.jobId = jobId;
  }

  // Starts the heartbeat loop
  async startHeartbeat(
    interval_s: number = 30,
    onCanceled: () => Promise<void>
  ): Promise<void> {
    this.active = true; // Set the loop to be active
    this.log.info("Heartbeat started.");

    while (this.active) {
      const { status } = await sendHeartbeat(this.jobId, this.log);
      this.numHeartbeats++;
      if (status === "canceled") {
        this.log.info("Job was canceled, stopping heartbeat.");
        await onCanceled();
        break;
      }
      if (state.getState().isUploadingFinalArtifacts === 0) {
        await setDeletionCost(this.numHeartbeats + 2, this.log);
      }
      this.waiter = sleep(interval_s * 1000);
      await this.waiter; // Wait for 30 seconds before the next heartbeat
    }

    this.log.info(`Heartbeat stopped.`);
  }

  // Stops the heartbeat loop
  async stopHeartbeat(): Promise<void> {
    this.log.info("Stopping heartbeat");
    this.active = false; // Set the loop to be inactive
    if (this.waiter) {
      await this.waiter; // Wait for the last heartbeat to complete
      this.waiter = null;
    }
  }
}

export async function setDeletionCost(
  cost: number,
  log: Logger
): Promise<void> {
  log.info(`Setting deletion cost to ${cost}`);
  try {
    const resp = await fetch(`${imdsUrl}/v1/deletion-cost`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        Metadata: "true",
      },
      body: JSON.stringify({ deletion_cost: cost }),
    });
    if (!resp.ok) {
      const body = await resp.text();
      log.error(`${resp.status}: Failed to set deletion cost: ${body}`);
    }
  } catch (error: any) {
    log.error(`Failed to set deletion cost: ${error.message}`);
  }
}

export async function recreateMe(log: Logger): Promise<void> {
  log.info("Recreating container via IMDS");
  try {
    await imds.metadata.recreate();
  } catch (e: any) {
    log.error(`Failed to recreate container via IMDS: ${e.message}`);
  }
}

async function getSaladJWT(
  log: Logger
): Promise<{ token: string; header: any; payload: any }> {
  try {
    const { jwt: token } = await fetchUpToNTimes<{ jwt: string }>(
      `${imdsUrl}/v1/token`,
      {
        method: "GET",
        headers: {
          Metadata: "true",
        },
      },
      3,
      log
    );
    const { header, payload } = decodeJWT(token);
    return { token, header, payload };
  } catch (e: any) {
    log.error(e.message);
    throw new Error("Failed to get token");
  }
}

function decodeJWT(token: string): { header: any; payload: any } {
  const [header, payload, signature] = token.split(".");
  const decodedHeader = Buffer.from(header, "base64").toString();
  const decodedPayload = Buffer.from(payload, "base64").toString();
  return {
    header: JSON.parse(decodedHeader),
    payload: JSON.parse(decodedPayload),
  };
}
