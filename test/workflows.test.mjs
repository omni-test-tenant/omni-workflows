import assert from "node:assert/strict";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { OmniWorkflowRunner } from "../src/omni-runner.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));

test("OmniWorkflowRunner executes order-settlement workflow sequentially", async () => {
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
});

test("OmniWorkflowRunner executes automated-bug-repro pipeline with custom handlers", async () => {
  const customEvents = [];
  const runner = new OmniWorkflowRunner({
    actionHandlers: {
      "customer-data.provisionCompositeTenantEmulation": async () => {
        customEvents.push("provision");
        return { sandboxReady: true };
      },
      "customer-data.replayScenario": async () => {
        customEvents.push("replay");
        return { raceDetected: true };
      }
    }
  });

  const workflowPath = join(__dirname, "../workflows/automated-bug-repro.yaml");
  const result = await runner.runWorkflow(workflowPath, { issueId: "ROCK-999" });

  assert.equal(result.success, true);
  assert.equal(result.workflowName, "automated-bug-repro");
  assert.equal(result.executedSteps.length, 5);
  assert.deepEqual(customEvents, ["provision", "replay"]);
  assert.equal(result.output.sandboxReady, true);
  assert.equal(result.output.raceDetected, true);
});
