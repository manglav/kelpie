import assert from "node:assert/strict";
import test from "node:test";

import {
  extractJobMetadata,
  extractRunNameFromS3Output,
} from "./jobMetadata";
import { Task } from "./types";

function task(overrides: Partial<Task>): Task {
  return {
    id: "job-1",
    user_id: "user-1",
    status: "running",
    created: new Date().toISOString(),
    num_failures: 0,
    machine_id: "machine-1",
    command: "python3",
    arguments: [],
    environment: {},
    heartbeat_interval: 30,
    max_failures: 3,
    container_group_id: "group-1",
    ...overrides,
  };
}

test("extractRunNameFromS3Output handles slash-delimited screening run names", () => {
  assert.equal(
    extractRunNameFromS3Output(
      "s3://docking-results/runs/CHK1/registry-pocket/enamine-real-2026-01-13-6m-raw/outputs/"
    ),
    "CHK1/registry-pocket/enamine-real-2026-01-13-6m-raw"
  );
});

test("extractJobMetadata extracts screening job identifiers", () => {
  const metadata = extractJobMetadata(
    task({
      command: "python3",
      arguments: [
        "/opt/screening/screen_shard.py",
        "--shard-index",
        "7527",
        "--s3-output",
        "s3://docking-results/runs/CHK1/registry-pocket/enamine-real-2026-01-13-6m-raw/outputs/",
        "--work-dir",
        "/app/data/output/work/shard_007527",
      ],
    })
  );

  assert.deepEqual(metadata, {
    command: "python3",
    argument_count: 7,
    run_name: "CHK1/registry-pocket/enamine-real-2026-01-13-6m-raw",
    shard: 7527,
    s3_output:
      "s3://docking-results/runs/CHK1/registry-pocket/enamine-real-2026-01-13-6m-raw/outputs/",
    work_dir: "/app/data/output/work/shard_007527",
  });
});

test("extractJobMetadata returns null identifiers for generic jobs", () => {
  const metadata = extractJobMetadata(
    task({
      command: "/bin/bash",
      arguments: ["-lc", "echo hello"],
    })
  );

  assert.equal(metadata.command, "/bin/bash");
  assert.equal(metadata.argument_count, 2);
  assert.equal(metadata.run_name, null);
  assert.equal(metadata.shard, null);
  assert.equal(metadata.s3_output, null);
  assert.equal(metadata.work_dir, null);
});
