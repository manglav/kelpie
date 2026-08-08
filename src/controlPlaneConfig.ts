export type ControlPlaneConfig = {
  apiRequestTimeoutMs: number;
  apiRetryInitialDelayMs: number;
  apiRetryMaxDelayMs: number;
  jobHeartbeatIntervalMs: number;
  maxAttempts: number;
  workPollIntervalMs: number;
};

const DEFAULT_API_REQUEST_TIMEOUT_S = 10;
const DEFAULT_API_RETRY_INITIAL_DELAY_S = 5;
const DEFAULT_API_RETRY_MAX_DELAY_S = 30;
const DEFAULT_JOB_HEARTBEAT_INTERVAL_S = 30;
const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_WORK_POLL_INTERVAL_S = 30;

function parsePositiveSeconds(
  value: string | undefined,
  defaultValue: number,
  variableName: string
): number {
  const parsed = value === undefined ? defaultValue : Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${variableName} must be a positive number`);
  }
  return parsed;
}

function parsePositiveInteger(
  value: string | undefined,
  defaultValue: number,
  variableName: string
): number {
  const parsed = value === undefined ? defaultValue : Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${variableName} must be a positive integer`);
  }
  return parsed;
}

export function loadControlPlaneConfig(
  env: NodeJS.ProcessEnv
): ControlPlaneConfig {
  const apiRetryInitialDelayS = parsePositiveSeconds(
    env.KELPIE_API_RETRY_INITIAL_DELAY_S,
    DEFAULT_API_RETRY_INITIAL_DELAY_S,
    "KELPIE_API_RETRY_INITIAL_DELAY_S"
  );
  const apiRetryMaxDelayS = parsePositiveSeconds(
    env.KELPIE_API_RETRY_MAX_DELAY_S,
    DEFAULT_API_RETRY_MAX_DELAY_S,
    "KELPIE_API_RETRY_MAX_DELAY_S"
  );
  if (apiRetryMaxDelayS < apiRetryInitialDelayS) {
    throw new Error(
      "KELPIE_API_RETRY_MAX_DELAY_S must be greater than or equal to KELPIE_API_RETRY_INITIAL_DELAY_S"
    );
  }

  const workPollIntervalS = parsePositiveSeconds(
    env.KELPIE_WORK_POLL_INTERVAL_S ?? env.HEARTBEAT_INTERVAL_S,
    DEFAULT_WORK_POLL_INTERVAL_S,
    env.KELPIE_WORK_POLL_INTERVAL_S === undefined &&
      env.HEARTBEAT_INTERVAL_S !== undefined
      ? "HEARTBEAT_INTERVAL_S"
      : "KELPIE_WORK_POLL_INTERVAL_S"
  );

  return {
    apiRequestTimeoutMs:
      parsePositiveSeconds(
        env.KELPIE_API_REQUEST_TIMEOUT_S,
        DEFAULT_API_REQUEST_TIMEOUT_S,
        "KELPIE_API_REQUEST_TIMEOUT_S"
      ) * 1000,
    apiRetryInitialDelayMs: apiRetryInitialDelayS * 1000,
    apiRetryMaxDelayMs: apiRetryMaxDelayS * 1000,
    jobHeartbeatIntervalMs:
      parsePositiveSeconds(
        env.KELPIE_JOB_HEARTBEAT_INTERVAL_S,
        DEFAULT_JOB_HEARTBEAT_INTERVAL_S,
        "KELPIE_JOB_HEARTBEAT_INTERVAL_S"
      ) * 1000,
    maxAttempts: parsePositiveInteger(
      env.MAX_RETRIES,
      DEFAULT_MAX_ATTEMPTS,
      "MAX_RETRIES"
    ),
    workPollIntervalMs: workPollIntervalS * 1000,
  };
}

export function retryDelayMs(
  failedAttemptNumber: number,
  config: Pick<
    ControlPlaneConfig,
    "apiRetryInitialDelayMs" | "apiRetryMaxDelayMs"
  >
): number {
  return Math.min(
    config.apiRetryInitialDelayMs * 2 ** (failedAttemptNumber - 1),
    config.apiRetryMaxDelayMs
  );
}

export const controlPlaneConfig = loadControlPlaneConfig(process.env);
