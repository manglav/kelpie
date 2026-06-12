import assert from "node:assert/strict";
import test from "node:test";

import { decideJobExitAction } from "./jobOutcome";

function legacyWouldReportFailure(exitCode: number | null): boolean {
  return exitCode !== 0;
}

test("successful uncanceled work completes", () => {
  const decision = decideJobExitAction(0, false);
  assert.equal(decision.action, "completed");
  assert.equal(decision.reportFailure, false);
});

test("nonzero uncanceled work reports failure", () => {
  const decision = decideJobExitAction(2, false);
  assert.equal(decision.action, "failed");
  assert.equal(decision.reportFailure, true);
});

test("canceled nonzero work no longer reports failure", () => {
  const exitCode = 130;

  assert.equal(legacyWouldReportFailure(exitCode), true);

  const decision = decideJobExitAction(exitCode, true);
  assert.equal(decision.action, "canceled");
  assert.equal(decision.reportFailure, false);
  assert.equal(decision.exitCode, exitCode);
});

test("canceled zero-exit work remains canceled", () => {
  const decision = decideJobExitAction(0, true);
  assert.equal(decision.action, "canceled");
  assert.equal(decision.reportFailure, false);
});
