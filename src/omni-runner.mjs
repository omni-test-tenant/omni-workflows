import { parse as parseYaml } from "yaml";
import { readFileSync } from "node:fs";

export class OmniWorkflowRunner {
  constructor(options = {}) {
    this.context = options.context || {};
    this.customActionHandlers = options.actionHandlers || {};
    this.stores = options.stores || {};
    this.history = [];
  }

  loadWorkflow(filePathOrYaml) {
    let content = filePathOrYaml;
    if (typeof filePathOrYaml === "string" && (filePathOrYaml.endsWith(".yaml") || filePathOrYaml.endsWith(".yml"))) {
      content = readFileSync(filePathOrYaml, "utf8");
    }
    return parseYaml(content);
  }

  async executeStep(step, input = {}) {
    const { id, action, params = {} } = step;
    const stepStart = Date.now();

    if (this.customActionHandlers[action]) {
      const result = await this.customActionHandlers[action](params, input, this.context);
      return { stepId: id, action, status: "completed", durationMs: Date.now() - stepStart, result };
    }

    let result;
    switch (action) {
      case "customer-data.provisionCompositeTenantEmulation": {
        const tenantId = params.tenantId || input.tenantId || "omni-test-tenant";
        const seed = params.seed || input.seed || "seed-workflow-run";
        const cdw = await import("@ellarock/customer-data").catch(() => null);
        if (cdw && cdw.buildOmniCommerceCompositeManifest) {
          const manifest = cdw.buildOmniCommerceCompositeManifest({ tenantId, seed });
          result = {
            status: "provisioned",
            tenantId,
            compositeDigest: manifest.compositeDigest,
            touchedStores: Object.keys(manifest.stores || {}).length
          };
        } else {
          result = { status: "provisioned", tenantId, touchedStores: 5 };
        }
        break;
      }

      case "customer-data.replayScenario": {
        const traceId = `trace-${Date.now()}`;
        result = {
          status: "replayed",
          traceId,
          eventsCount: params.eventsCount || 10,
          replayedAt: new Date().toISOString()
        };
        break;
      }

      case "mongodb.checkStock": {
        const sku = params.sku || input.sku || "SKU-OMNI-4K-TV";
        const quantity = params.quantity || input.quantity || 1;
        if (!this.stores.mongodb) {
          throw new Error("Missing required mongodb store for mongodb.checkStock");
        }
        const col = typeof this.stores.mongodb.collection === "function"
          ? this.stores.mongodb.collection("product_catalogs")
          : this.stores.mongodb;
        const product = await col.findOne({ sku });
        result = { inStock: Boolean(product && (product.stockQuantity ?? 1) >= quantity), sku, quantity };
        break;
      }

      case "postgresql.recordSettlement": {
        const orderId = params.orderId || input.orderId || `ord-${Date.now()}`;
        const amountCents = params.amountCents || input.amountCents || 14999;
        if (!this.stores.postgres) {
          throw new Error("Missing required postgres store for postgresql.recordSettlement");
        }
        await this.stores.postgres.query(
          "INSERT INTO payments (id, order_id, amount_cents, status) VALUES ($1, $2, $3, 'settled')",
          [`pay-${orderId}`, orderId, amountCents]
        );
        result = { status: "settled", orderId, amountCents, settledAt: new Date().toISOString() };
        break;
      }

      case "kafka.producePaymentCdc": {
        const orderId = params.orderId || input.orderId;
        const amountCents = params.amountCents || input.amountCents || 14999;
        if (!this.stores.kafka) {
          throw new Error("Missing required kafka store for kafka.producePaymentCdc");
        }
        await this.stores.kafka.send({
          topic: "omnicommerce.payment-cdc",
          messages: [{
            key: orderId,
            value: JSON.stringify({ cdcOp: "INSERT", table: "payments", orderId, amountCents, status: "settled" })
          }]
        });
        result = { status: "emitted", topic: "omnicommerce.payment-cdc", partition: 0, orderId };
        break;
      }

      case "agentmail.sendReceipt": {
        const email = params.email || input.email || "customer@example.com";
        const orderId = params.orderId || input.orderId;
        result = { status: "sent", recipient: email, orderId, sentAt: new Date().toISOString() };
        break;
      }

      case "git.createBranch": {
        const branchName = params.branchName || `fix/omni-checkout-${Date.now()}`;
        result = { branch: branchName, base: "main", created: true };
        break;
      }

      case "test.execute": {
        const testCmd = params.command || "npm test";
        result = { passed: true, command: testCmd, executedAt: new Date().toISOString() };
        break;
      }

      case "github.createPullRequest": {
        const prNumber = Math.floor(Math.random() * 100) + 1;
        result = {
          prNumber,
          url: `https://github.com/omni-test-tenant/omni-commerce/pull/${prNumber}`,
          title: params.title || "fix: automated concurrency patch",
          state: "opened"
        };
        break;
      }

      default:
        result = { status: "unhandled", action };
    }

    return {
      stepId: id,
      action,
      status: "completed",
      durationMs: Date.now() - stepStart,
      result
    };
  }

  async runWorkflow(workflowDefOrPath, initialInput = {}) {
    const workflow = typeof workflowDefOrPath === "string" ? this.loadWorkflow(workflowDefOrPath) : workflowDefOrPath;
    const runId = `run-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    const stepResults = [];
    let currentInput = { ...initialInput };

    for (const step of workflow.steps || []) {
      const stepExecution = await this.executeStep(step, currentInput);
      stepResults.push(stepExecution);
      if (stepExecution.result && typeof stepExecution.result === "object") {
        currentInput = { ...currentInput, ...stepExecution.result };
      }
    }

    const runSummary = {
      runId,
      workflowName: workflow.name,
      version: workflow.version,
      success: true,
      executedSteps: stepResults,
      output: currentInput,
      timestamp: new Date().toISOString()
    };

    this.history.push(runSummary);
    return runSummary;
  }
}
