import assert from "node:assert/strict";
import path from "node:path";
import test from "node:test";

import pino from "pino";

import { uploadDirectory } from "./s3";

const log = pino({ enabled: false });
const missingDirectory = path.join(
  "/tmp",
  `kelpie-missing-upload-directory-${process.pid}`
);

test("best-effort directory uploads retain existing error behavior", async () => {
  await assert.doesNotReject(
    uploadDirectory({
      jobId: "best-effort-upload",
      directory: missingDirectory,
      bucket: "unused",
      prefix: "unused/",
      batchSize: 10,
      compress: false,
      log,
    })
  );
});

test("required directory uploads propagate errors", async () => {
  await assert.rejects(
    uploadDirectory({
      jobId: "required-upload",
      directory: missingDirectory,
      bucket: "unused",
      prefix: "unused/",
      batchSize: 10,
      compress: false,
      log,
      throwOnError: true,
    }),
    /ENOENT/
  );
});
