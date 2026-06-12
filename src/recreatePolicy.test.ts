import assert from "node:assert/strict";
import test from "node:test";

import {
  decideRecreateAfterJob,
  parseRecreateEveryNJobs,
} from "./recreatePolicy";

test("parseRecreateEveryNJobs treats zero, negative, and invalid values as disabled", () => {
  assert.equal(parseRecreateEveryNJobs("0"), 0);
  assert.equal(parseRecreateEveryNJobs("-3"), 0);
  assert.equal(parseRecreateEveryNJobs("not-a-number"), 0);
});

test("parseRecreateEveryNJobs accepts positive integers", () => {
  assert.equal(parseRecreateEveryNJobs("1"), 1);
  assert.equal(parseRecreateEveryNJobs("50"), 50);
});

test("recreate between jobs forces recreate after every job", () => {
  const decision = decideRecreateAfterJob({
    recreateBetweenJobs: true,
    recreateEveryNJobs: 0,
    jobsSinceRecreate: 1,
  });

  assert.deepEqual(decision, {
    shouldRecreate: true,
    reason: "between_jobs",
  });
});

test("recreate every N jobs waits until the threshold", () => {
  assert.deepEqual(
    decideRecreateAfterJob({
      recreateBetweenJobs: false,
      recreateEveryNJobs: 3,
      jobsSinceRecreate: 2,
    }),
    { shouldRecreate: false, reason: null }
  );

  assert.deepEqual(
    decideRecreateAfterJob({
      recreateBetweenJobs: false,
      recreateEveryNJobs: 3,
      jobsSinceRecreate: 3,
    }),
    { shouldRecreate: true, reason: "every_n_jobs" }
  );
});

test("recreate between jobs takes precedence when both policies are set", () => {
  const decision = decideRecreateAfterJob({
    recreateBetweenJobs: true,
    recreateEveryNJobs: 50,
    jobsSinceRecreate: 1,
  });

  assert.deepEqual(decision, {
    shouldRecreate: true,
    reason: "between_jobs",
  });
});
