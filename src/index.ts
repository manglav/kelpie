import { mkdirSync } from "fs";
import { randomUUID } from "crypto";

import {
  getWork,
  HeartbeatManager,
  reportFailed,
  reportCompleted,
  reallocateMe,
  recreateMe,
  setDeletionCost,
} from "./api";
import { DirectoryWatcher, purgeDirectory } from "./files";
import {
  downloadAllFilesFromPrefix,
  uploadDirectory,
  uploadFile,
  deleteFile,
  downloadSyncConfig,
  uploadSyncConfig,
} from "./s3";
import {
  CommandExecutor,
  ProcessGroupCleanupResult,
  StopSignalStep,
} from "./commands";
import path from "path";
import { version } from "../package.json";
import fs from "fs/promises";
import { log as baseLogger } from "./logger";
import { Logger } from "pino";
import { SyncConfig, Task } from "./types";
import state from "./state";
import { decideJobExitAction } from "./jobOutcome";
import {
  decideRecreateAfterJob,
  parseRecreateEveryNJobs,
} from "./recreatePolicy";
import { extractJobMetadata } from "./jobMetadata";

const {
  INPUT_DIR = "/input",
  OUTPUT_DIR = "/output",
  CHECKPOINT_DIR = "/checkpoint",

  // Default to 0, which means no timeout
  MAX_TIME_WITH_NO_WORK_S = "0",

  // There are backend implications to this, so we aren't documenting it yet.
  HEARTBEAT_INTERVAL_S = "10",

  KELPIE_CANCEL_PROGRESS_LOG_INTERVAL_S = "10",
  KELPIE_CANCEL_SIGINT_GRACE_S = "15",
  KELPIE_CANCEL_SIGTERM_GRACE_S = "15",
  KELPIE_CANCEL_SIGKILL_GRACE_S = "3",

  KELPIE_RECREATE_BETWEEN_JOBS = "false",
  KELPIE_RECREATE_EVERY_N_JOBS = "0",
  KELPIE_RECREATE_AFTER_CANCELED_JOB = "false",
  SCREENING_WORKER_START_ID = "unknown",
  SALAD_MACHINE_ID = "unknown",
  SALAD_CONTAINER_GROUP_ID = "unknown",
} = process.env;

mkdirSync(INPUT_DIR, { recursive: true });
mkdirSync(OUTPUT_DIR, { recursive: true });
mkdirSync(CHECKPOINT_DIR, { recursive: true });

const maxTimeWithNoWorkMs = parseInt(MAX_TIME_WITH_NO_WORK_S, 10) * 1000;
const heartbeatIntervalMs = parseInt(HEARTBEAT_INTERVAL_S, 10) * 1000;
const cancelProgressLogIntervalMs =
  parseInt(KELPIE_CANCEL_PROGRESS_LOG_INTERVAL_S, 10) * 1000;
const recreateBetweenJobs = KELPIE_RECREATE_BETWEEN_JOBS === "true";
const recreateEveryNJobs = parseRecreateEveryNJobs(
  KELPIE_RECREATE_EVERY_N_JOBS
);
const recreateAfterCanceledJob = KELPIE_RECREATE_AFTER_CANCELED_JOB === "true";

function parseNonNegativeSeconds(value: string, fallbackSeconds: number): number {
  const parsed = Number.parseFloat(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallbackSeconds;
  }
  return parsed;
}

const cancelStopSequence: StopSignalStep[] = [
  {
    signal: "SIGINT",
    graceMs:
      parseNonNegativeSeconds(KELPIE_CANCEL_SIGINT_GRACE_S, 15) * 1000,
  },
  {
    signal: "SIGTERM",
    graceMs:
      parseNonNegativeSeconds(KELPIE_CANCEL_SIGTERM_GRACE_S, 15) * 1000,
  },
  {
    signal: "SIGKILL",
    graceMs:
      parseNonNegativeSeconds(KELPIE_CANCEL_SIGKILL_GRACE_S, 3) * 1000,
  },
];

const commandExecutor = new CommandExecutor();

function workerIdentityFields() {
  return {
    screening_worker_start_id: SCREENING_WORKER_START_ID || "unknown",
    machine_id: SALAD_MACHINE_ID || "unknown",
    container_group_id: SALAD_CONTAINER_GROUP_ID || "unknown",
  };
}

async function clearAllDirectories(dirsToClear: string[]): Promise<void> {
  await Promise.all(dirsToClear.map((dir) => purgeDirectory(dir, baseLogger)));
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function uploadAndCompleteJob(
  work: Task,
  dirToUpload: string,
  heartbeatManager: HeartbeatManager,
  log: Logger
): Promise<void> {
  state.getState().isUploadingFinalArtifacts++;
  log.info(`Uploading output directory: ${dirToUpload}`);
  try {
    await setDeletionCost(999999999, log); // Increase deletion cost to prevent instance from being deleted while uploading
    await uploadDirectory({
      jobId: work.id,
      directory: dirToUpload,
      bucket: work.output_bucket!,
      prefix: work.output_prefix!,
      batchSize: 2,
      compress: !!work.compression,
      log,
    });
  } catch (e: any) {
    log.error(`Error uploading output directory: ${e.message}`);
    await reportFailed(work.id, log);
    state.getState().isUploadingFinalArtifacts--;
    await heartbeatManager.stopHeartbeat();
    return;
  }

  try {
    await reportCompleted(work.id, log);
    state.getState().isUploadingFinalArtifacts--;
  } catch (e: any) {
    log.error(`Error reporting job completion: ${e.message}`);
    state.getState().isUploadingFinalArtifacts--;
    await heartbeatManager.stopHeartbeat();
    return;
  }

  log.info(
    `Output directory uploaded and job completed. Removing ${dirToUpload}...`
  );
  await fs.rmdir(dirToUpload, { recursive: true });
  await heartbeatManager.stopHeartbeat();
}

let keepAlive = true;
process.on("SIGINT", () => {
  baseLogger.info("Received SIGINT, stopping...");
  keepAlive = false;
});

process.on("SIGTERM", () => {
  keepAlive = false;
  baseLogger.info("Received SIGTERM, stopping...");
  process.exit();
});

const filesBeingSynced = new Set();

async function main() {
  baseLogger.info(`Kelpie v${version} started`);
  await state.saveState(baseLogger);
  await clearAllDirectories(
    Array.from(new Set([INPUT_DIR, OUTPUT_DIR, CHECKPOINT_DIR]))
  );

  let lastWorkReceived = Date.now();
  let jobsSinceRecreate = 0;
  while (keepAlive) {
    let work;
    try {
      work = await getWork();
    } catch (e: any) {
      baseLogger.error("Error fetching work: ", e);
      await sleep(heartbeatIntervalMs);
      continue;
    }

    if (!work) {
      if (
        maxTimeWithNoWorkMs > 0 &&
        Date.now() - lastWorkReceived > maxTimeWithNoWorkMs
      ) {
        baseLogger.info(
          `No work received for ${
            maxTimeWithNoWorkMs / 1000
          } seconds, exiting...`
        );
        keepAlive = false;
        /**
         * A common reason to have no work for too long is that the instance is
         * banned from a particular workload. In this case, we should reallocate
         * the instance to get a new machine id. However, we want to make sure any uploads
         * that are currently in progress complete.
         */
        const currentState = state.getState();
        let uploadsInProgress = false;
        await Promise.all(
          currentState.jobs.map(async (job) => {
            if (job.activeUploads.size) {
              uploadsInProgress = true;
              baseLogger.info(
                `Waiting for uploads to finish before reallocation...`
              );
              await state.waitForUploads(job.id, baseLogger);
            }
          })
        );

        if (uploadsInProgress) {
          /**
           * If there were uploads in progress, we should check for work one more time
           */
          continue;
        }

        /**
         * If there are no uploads in progress, we can reallocate the instance.
         */
        await reallocateMe(
          `Kelpie: Max idle time exceeded: ${maxTimeWithNoWorkMs}`,
          baseLogger
        );
        break;
      }
      baseLogger.info("No work available, sleeping for 10 seconds...");
      if (state.getState().isUploadingFinalArtifacts === 0) {
        // If no uploads are in progress, we can reset the deletion cost
        await setDeletionCost(0, baseLogger);
      }
      await sleep(heartbeatIntervalMs);
      continue;
    }
    lastWorkReceived = Date.now();
    const jobMetadata = extractJobMetadata(work);
    const jobAttemptId = randomUUID();
    const jobStartedAtMs = Date.now();
    let observedExitCode: number | null = null;
    let observedExitAction: string | null = null;
    let observedExitError: string | null = null;
    const log = baseLogger.child({
      job_id: work.id,
      kelpie_job_attempt_id: jobAttemptId,
      run_name: jobMetadata.run_name,
      shard: jobMetadata.shard,
      screening_worker_start_id: SCREENING_WORKER_START_ID || "unknown",
    });
    log.info(
      {
        ...jobMetadata,
        ...workerIdentityFields(),
        kelpie_job_attempt_id: jobAttemptId,
        task_container_group_id: work.container_group_id,
        job_started_at_ms: jobStartedAtMs,
        heartbeat_interval_s: work.heartbeat_interval,
        max_failures: work.max_failures,
      },
      "kelpie_job_received"
    );
    state.startJob(work.id, log);
    if (state.getState().isUploadingFinalArtifacts === 0) {
      await setDeletionCost(1, log);
    }

    log.info("Starting heartbeat manager...");
    const heartbeatManager = new HeartbeatManager(work.id, log);

    const directoryWatchers: DirectoryWatcher[] = [];

    /**
     * The heartbeat endpoint may return a status of "canceled" if the job has been cancelled,
     * in which case we should stop the job and ask for a new one.
     */
    let jobWasCanceled = false;
    let lateCancellationAfterProcessExit = false;
    let cancelDetectedAtMs: number | null = null;
    let cancelCleanupResult: ProcessGroupCleanupResult | null = null;
    let cancelCleanupPromise: Promise<ProcessGroupCleanupResult> | null = null;
    let cancelProgressTimer: NodeJS.Timeout | null = null;
    const stopCancelProgressLogging = () => {
      if (cancelProgressTimer) {
        clearInterval(cancelProgressTimer);
        cancelProgressTimer = null;
      }
    };

    const onJobCancel = async () => {
      cancelDetectedAtMs = Date.now();
      await Promise.all(
        directoryWatchers.map((watcher) => watcher.stopWatching())
      );
      const runningJob = commandExecutor.getRunningJob();
      const cancelSignalTarget = runningJob ? "process_group" : "none";
      log.info(
        {
          canceled: true,
          cancel_detected_at_ms: cancelDetectedAtMs,
          recreate_after_canceled_job: recreateAfterCanceledJob,
          cancel_signal_target: cancelSignalTarget,
          cancel_signal_pid: runningJob?.pid,
          cancel_signal_pgid: runningJob?.pgid,
        },
        "remote_cancellation_observed"
      );

      if (!runningJob) {
        lateCancellationAfterProcessExit = true;
        cancelCleanupPromise = commandExecutor.stopProcessGroup({
          reason: "late_remote_cancel_after_process_exit",
          sequence: cancelStopSequence,
          logger: log,
        });
        cancelCleanupResult = await cancelCleanupPromise;
        log.info(
          {
            canceled: true,
            cancel_detected_at_ms: cancelDetectedAtMs,
            cancel_signal_target: cancelSignalTarget,
            cancel_cleanup_group_empty: cancelCleanupResult.groupEmpty,
            cancel_cleanup_elapsed_ms: cancelCleanupResult.cleanupElapsedMs,
          },
          "late_remote_cancellation_after_process_exit"
        );
        return;
      }

      jobWasCanceled = true;
      stopCancelProgressLogging();
      if (cancelProgressLogIntervalMs > 0) {
        cancelProgressTimer = setInterval(() => {
          const nowMs = Date.now();
          log.warn(
            {
              canceled: true,
              cancel_detected_at_ms: cancelDetectedAtMs,
              cancel_wait_ms: nowMs - cancelDetectedAtMs!,
              cancel_signal_target: runningJob ? "process_group" : "none",
              cancel_signal_pid: runningJob?.pid,
              cancel_signal_pgid: runningJob?.pgid,
            },
            "Remote cancellation still waiting for process exit"
          );
        }, cancelProgressLogIntervalMs);
      }

      cancelCleanupPromise = commandExecutor.stopProcessGroup({
        reason: "remote_cancel",
        sequence: cancelStopSequence,
        logger: log,
      });
      cancelCleanupResult = await cancelCleanupPromise;
    };

    const handleHeartbeatError = async (e: any) => {
      /**
       * This occurs if a heartbeat fails config.maxRetries times, meaning the machine
       * has lost communication with kelpie api
       *  */
      log.error(`Heartbeat error: ${e.message}`);

      /**
       * If the heartbeat throws an error, we should restart it.
       * This is because the error is likely due to a network issue which
       * may be transient, and the job is still running. This way,
       * the job can continue to run and the heartbeat will be re-established.
       *
       * The alternative is to abort the job or reallocate the instance, but this is
       * not ideal because the job is still running and may complete successfully.
       */
      await heartbeatManager.stopHeartbeat();
      await heartbeatManager
        .startHeartbeat(work.heartbeat_interval, onJobCancel)
        .catch(handleHeartbeatError);
    };

    heartbeatManager
      .startHeartbeat(work.heartbeat_interval, onJobCancel)
      .catch(handleHeartbeatError);

    /**
     * This block is event-driven, triggered by file changes in configured directories.
     */
    if (work.sync) {
      if (work.sync.before && work.sync.before.length) {
        for (const syncConfig of work.sync.before) {
          await downloadSyncConfig(
            work.id,
            syncConfig,
            !!work.compression,
            log
          );
        }
      }

      if (work.sync.during && work.sync.during.length) {
        for (const syncConfig of work.sync.during) {
          const dirWatcher = new DirectoryWatcher(syncConfig.local_path, log);
          dirWatcher.watchDirectory(
            async (localFilePath: string, eventType: string) => {
              if (filesBeingSynced.has(localFilePath)) {
                return;
              }
              const relativeFilename = path.relative(
                syncConfig.local_path,
                localFilePath
              );
              if (
                (eventType === "add" || eventType === "change") &&
                syncConfig.direction === "upload" &&
                (!syncConfig.pattern ||
                  new RegExp(syncConfig.pattern).test(relativeFilename))
              ) {
                filesBeingSynced.add(localFilePath);
                await uploadFile(
                  work.id,
                  localFilePath,
                  syncConfig.bucket,
                  syncConfig.prefix + relativeFilename,
                  !!work.compression,
                  log
                );
                filesBeingSynced.delete(localFilePath);
              } else if (
                eventType == "unlink" &&
                syncConfig.direction === "upload" &&
                (!syncConfig.pattern ||
                  new RegExp(syncConfig.pattern).test(relativeFilename))
              ) {
                filesBeingSynced.add(localFilePath);
                let keyToDelete = syncConfig.prefix + relativeFilename;
                if (!!work.compression) {
                  keyToDelete += ".gz";
                }
                await deleteFile(syncConfig.bucket, keyToDelete, log);
                filesBeingSynced.delete(localFilePath);
              }
            }
          );
          directoryWatchers.push(dirWatcher);
        }
      }
    } else if (work.input_bucket && work.input_prefix) {
      // Download required files
      if (work.input_bucket && work.input_prefix) {
        try {
          await downloadAllFilesFromPrefix({
            jobId: work.id,
            bucket: work.input_bucket,
            prefix: work.input_prefix,
            outputDir: INPUT_DIR,
            batchSize: 20,
            decompress: !!work.compression,
            log,
          });
        } catch (e: any) {
          log.error(`Error downloading input files: ${e.message}`);
          // await reportFailed(work.id);
          continue;
        }
      }

      if (work.checkpoint_bucket && work.checkpoint_prefix) {
        try {
          await downloadAllFilesFromPrefix({
            jobId: work.id,
            bucket: work.checkpoint_bucket,
            prefix: work.checkpoint_prefix,
            outputDir: CHECKPOINT_DIR,
            batchSize: 20,
            decompress: !!work.compression,
            log,
          });
        } catch (e: any) {
          log.error(`Error downloading checkpoint files: ${e.message}`);
          // await reportFailed(work.id);
          continue;
        }
        const checkpointWatcher = new DirectoryWatcher(CHECKPOINT_DIR, log);

        checkpointWatcher.watchDirectory(
          async (localFilePath: string, eventType: string) => {
            const relativeFilename = path.relative(
              CHECKPOINT_DIR,
              localFilePath
            );
            if (eventType === "add" || eventType === "change") {
              await uploadFile(
                work.id,
                localFilePath,
                work.checkpoint_bucket!,
                work.checkpoint_prefix + relativeFilename,
                !!work.compression,
                log
              );
            } else if (eventType === "unlink") {
              await deleteFile(
                work.checkpoint_bucket!,
                work.checkpoint_prefix + relativeFilename,
                log
              );
            }
          }
        );

        directoryWatchers.push(checkpointWatcher);
      }

      log.info(
        "All files downloaded successfully, starting directory watchers..."
      );

      if (
        work.output_bucket &&
        work.output_prefix &&
        CHECKPOINT_DIR !== OUTPUT_DIR
      ) {
        const outputWatcher = new DirectoryWatcher(OUTPUT_DIR, log);
        outputWatcher.watchDirectory(
          async (localFilePath: string, eventType: string) => {
            const relativeFilename = path.relative(OUTPUT_DIR, localFilePath);
            if (eventType === "add") {
              await uploadFile(
                work.id,
                localFilePath,
                work.output_bucket!,
                work.output_prefix + relativeFilename,
                !!work.compression,
                log
              );
            }
          }
        );
        directoryWatchers.push(outputWatcher);
      }
    } else {
      log.info("No storage configuration provided, skipping file sync");
    }

    /**
     * Run the command configured by the job, and then handle the outcome of that.
     */
    try {
      const exitCode = await commandExecutor.execute(
        work.command,
        work.arguments,
        {
          ...work.environment,
          INPUT_DIR,
          OUTPUT_DIR,
          CHECKPOINT_DIR,
          KELPIE_STATE_FILE: state.filename,
          KELPIE_JOB_ID: work.id,
          KELPIE_JOB_ATTEMPT_ID: jobAttemptId,
          SCREENING_JOB_ATTEMPT_ID: jobAttemptId,
          SALAD_JOB_ID: work.id,
        },
        log
      );
      observedExitCode = exitCode;
      /**
       * Once the command exits, we can update the job's status in the state.
       * In the event the exitCode is null, we will default to -2, which is
       * an error code that is not used by any system commands.
       */
      state.jobExited(work.id, exitCode ?? -2, log);
      /**
       * Once the script updates, we can stop watching the directories.
       * This will stop the event-driven file sync behavior that is
       * defined above, but it will not interrupt any ongoing uploads.
       */
      await Promise.all(
        directoryWatchers.map((watcher) => watcher.stopWatching())
      );

      /**
       * If the command exits with a 0 status code, we can consider the job
       * to be successful. Otherwise, we should report the job as failed.
       */
      const exitDecision = decideJobExitAction(exitCode, jobWasCanceled);
      observedExitAction = exitDecision.action;
      if (exitDecision.action === "canceled") {
        await heartbeatManager.stopHeartbeat();
        const exitedAtMs = Date.now();
        stopCancelProgressLogging();
        log.info(
          {
            canceled: true,
            exit_code: exitDecision.exitCode,
            report_failure: exitDecision.reportFailure,
            cancel_detected_at_ms: cancelDetectedAtMs,
            cancel_exited_at_ms: exitedAtMs,
            cancel_to_exit_ms:
              cancelDetectedAtMs === null ? null : exitedAtMs - cancelDetectedAtMs,
          },
          "Work exited after remote cancellation"
        );
      } else if (exitDecision.action === "completed") {
        log.info(`Work completed successfully on job ${work.id}`);

        // Sleep for a second to ensure the output files are written
        await sleep(1000);

        // Move the output directory to a separate location and upload it asynchronously
        if (!work.sync) {
          /**
           * THIS IS LEGACY BEHAVIOR.
           */
          const newDir = `/output-${work.id}`;
          await fs.rename(OUTPUT_DIR, newDir);
          await fs.mkdir(OUTPUT_DIR, { recursive: true });

          /**
           * Upload and complete and wait for them to complete.

           */
          await uploadAndCompleteJob(work, newDir, heartbeatManager, log);
        } else if (work.sync.after && work.sync.after.length) {
          /**
           * work.sync.after is an array of upload sync blocks.
           */
          // Move the output directory to a separate location and upload it asynchronously
          const modifiedOutputs: SyncConfig[] = [];
          for (let syncConfig of work.sync.after) {
            const newDir = `${path.resolve(syncConfig.local_path)}-${work.id}`;
            log.info(`Moving ${syncConfig.local_path} to ${newDir} for upload`);
            try {
              /**
               * Try moving the folder, because it's faster than copying.
               */
              await fs.rename(syncConfig.local_path, newDir);
            } catch (e: any) {
              /**
               * If the move fails, it's likely due to a cross-device link error,
               * so we should copy the folder instead.
               */
              if (e.code && e.code === "EXDEV") {
                log.warn(
                  `Cannot move ${syncConfig.local_path} to ${newDir} due to cross-device link, copying instead`
                );
                await fs.cp(syncConfig.local_path, newDir, { recursive: true });
                await fs.rm(syncConfig.local_path, { recursive: true });
              } else {
                throw e;
              }
            } finally {
              await fs.mkdir(syncConfig.local_path, { recursive: true });
            }

            modifiedOutputs.push({
              ...syncConfig,
              local_path: newDir,
            });
            log.info(`Moved ${syncConfig.local_path} to ${newDir} for upload`);
          }

          /**
           * Upload all sync configs and wait for them to complete.
           */
          await Promise.all(
            modifiedOutputs.map(async (syncConfig) => {
              await uploadSyncConfig(
                work.id,
                syncConfig,
                !!work.compression,
                log
              );
            })
          )
            .then(async () => {
              /**
               * Now that all uploads are complete, we can report the job as completed.
               * Only now do we stop the job's heartbeat, because otherwise the job may
               * be handed out again during final upload.
               */
              await heartbeatManager.stopHeartbeat();
              await reportCompleted(work.id, log);
            })
            .catch(async (e: any) => {
              log.error(`Error processing sync config: ${e.message}`);
              await reportFailed(work.id, log);
            })
            .finally(async () => {
              /**
               * Finally, we can clear the directories that were used for the sync.
               */
              await heartbeatManager.stopHeartbeat();
              await clearAllDirectories(
                modifiedOutputs.map((syncConfig) => syncConfig.local_path)
              );
            });
        } else {
          /**
           * If there's no IO to process at all, we can just report the job as completed.
           */
          await heartbeatManager.stopHeartbeat();
          await reportCompleted(work.id, log);
        }
      } else {
        await reportFailed(work.id, log);
        await heartbeatManager.stopHeartbeat();
        log.error(`Work failed with exit code ${exitCode}`);
      }
    } catch (e: any) {
      observedExitError = e.message;
      if (/terminated due to signal/i.test(e.message)) {
        if (jobWasCanceled) {
          observedExitAction = "canceled";
          const exitedAtMs = Date.now();
          stopCancelProgressLogging();
          log.info(
            {
              canceled: true,
              exit_code: null,
              report_failure: false,
              error: e.message,
              cancel_detected_at_ms: cancelDetectedAtMs,
              cancel_exited_at_ms: exitedAtMs,
              cancel_to_exit_ms:
                cancelDetectedAtMs === null ? null : exitedAtMs - cancelDetectedAtMs,
            },
            "Work exited after remote cancellation"
          );
        } else {
          observedExitAction = "interrupted";
          log.info("Work was interrupted, likely due to remote cancellation");
        }
      } else {
        observedExitAction = "failed";
        log.error(`Error processing work: ${e.message}`);
        await reportFailed(work.id, log);
      }
      stopCancelProgressLogging();
      await heartbeatManager.stopHeartbeat();
    }

    if (jobWasCanceled && !cancelCleanupResult) {
      cancelCleanupResult = await (cancelCleanupPromise ??
        commandExecutor.stopProcessGroup({
          reason: "remote_cancel_after_exit",
          sequence: cancelStopSequence,
          logger: log,
        }));
    }

    const jobEndedAtMs = Date.now();
    log.info(
      {
        ...jobMetadata,
        ...workerIdentityFields(),
        kelpie_job_attempt_id: jobAttemptId,
        task_container_group_id: work.container_group_id,
        job_started_at_ms: jobStartedAtMs,
        job_ended_at_ms: jobEndedAtMs,
        job_runtime_ms: jobEndedAtMs - jobStartedAtMs,
        exit_code: observedExitCode,
        exit_action: observedExitAction,
        error: observedExitError,
        canceled: jobWasCanceled,
        late_cancellation_after_process_exit: lateCancellationAfterProcessExit,
        cancel_detected_at_ms: cancelDetectedAtMs,
        cancel_to_exit_ms:
          cancelDetectedAtMs === null ? null : jobEndedAtMs - cancelDetectedAtMs,
        cancel_cleanup_group_empty: cancelCleanupResult?.groupEmpty,
        cancel_cleanup_final_signal: cancelCleanupResult?.finalSignal,
        cancel_cleanup_elapsed_ms: cancelCleanupResult?.cleanupElapsedMs,
      },
      "kelpie_job_exit"
    );

    /**
     * While the previous job is being finalized from temporary directories,
     * we can clear the directories that were used for the job, in preparation for the next job
     */
    await Promise.all(
      directoryWatchers.map((watcher) => watcher.stopWatching())
    );

    // Clear all directories, including the ones used for sync
    let dirsToClear = [INPUT_DIR, OUTPUT_DIR, CHECKPOINT_DIR];
    if (work.sync) {
      if (work.sync.before && work.sync.before.length) {
        dirsToClear.push(
          ...work.sync.before.map((syncConfig) => syncConfig.local_path)
        );
      }
      if (work.sync.during && work.sync.during.length) {
        dirsToClear.push(
          ...work.sync.during.map((syncConfig) => syncConfig.local_path)
        );
      }
      if (work.sync.after && work.sync.after.length) {
        dirsToClear.push(
          ...work.sync.after.map((syncConfig) => syncConfig.local_path)
        );
      }
    }
    // Remove duplicates
    dirsToClear = Array.from(new Set(dirsToClear));

    await clearAllDirectories(dirsToClear);

    jobsSinceRecreate++;
    const recreateDecision = decideRecreateAfterJob({
      jobWasCanceled,
      recreateAfterCanceledJob,
      recreateBetweenJobs,
      recreateEveryNJobs,
      jobsSinceRecreate,
    });

    if (recreateDecision.shouldRecreate) {
      await state.waitForUploads(work.id, baseLogger);
      baseLogger.info(
        {
          recreate_reason: recreateDecision.reason,
          jobs_since_recreate: jobsSinceRecreate,
          recreate_every_n_jobs: recreateEveryNJobs,
          job_was_canceled: jobWasCanceled,
          recreate_after_canceled_job: recreateAfterCanceledJob,
          cancel_cleanup_group_empty: cancelCleanupResult?.groupEmpty,
          cancel_cleanup_final_signal: cancelCleanupResult?.finalSignal,
          cancel_cleanup_elapsed_ms: cancelCleanupResult?.cleanupElapsedMs,
        },
        "container_recreate_requested"
      );
      await recreateMe(baseLogger);
      await sleep(1000); // Give some time for the container to be recreated
      break;
    }

    commandExecutor.clearRunningJob();
  }
}

main().then(() => baseLogger.info("Kelpie Exiting"));
