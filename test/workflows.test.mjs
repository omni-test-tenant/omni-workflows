import assert from "node:assert/strict";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { OmniWorkflowRunner } from "../src/omni-runner.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));

test("OmniWorkflowRunner executes order-settlement workflow sequentially with built-in handlers", async () => {
  const runner = new OmniWorkflowRunner();
  const workflowPath = join(__dirname, "../workflows/order-settlement.yaml");
  const result = await runner.runWorkflow(workflowPath, {
    orderId: "ord-settle-101",
    sku: "SKU-OMNI-4K-TV",
    amountCents: 14999
  });

  assert.equal(result.success, true);
  assert.equal(result.workflowName, "order-settlement");
  assert.equal(result.executedSteps.length, 4);
  assert.equal(result.executedSteps[0].action, "mongodb.checkStock");
  assert.equal(result.executedSteps[1].action, "postgresql.recordSettlement");
  assert.equal(result.executedSteps[2].action, "kafka.producePaymentCdc");
  assert.equal(result.executedSteps[3].action, "agentmail.sendReceipt");
  assert.equal(result.output.inStock, true);
  assert.equal(result.output.status, "sent");
});

test("OmniWorkflowRunner executes automated-bug-repro pipeline end-to-end with zero mocks", async () => {
  const runner = new OmniWorkflowRunner();
  const workflowPath = join(__dirname, "../workflows/automated-bug-repro.yaml");
  const result = await runner.runWorkflow(workflowPath, { issueId: "ROCK-999" });

  assert.equal(result.success, true);
  assert.equal(result.workflowName, "automated-bug-repro");
  assert.equal(result.executedSteps.length, 5);
  assert.equal(result.executedSteps[0].action, "customer-data.provisionCompositeTenantEmulation");
  assert.equal(result.executedSteps[1].action, "customer-data.replayScenario");
  assert.equal(result.executedSteps[2].action, "git.createBranch");
  assert.equal(result.executedSteps[3].action, "test.execute");
  assert.equal(result.executedSteps[4].action, "github.createPullRequest");
  assert.equal(result.output.passed, true);
  assert.ok(result.output.url);
});
