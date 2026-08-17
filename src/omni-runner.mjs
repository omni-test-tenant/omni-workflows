import { parse as parseYaml } from "yaml";
import { readFileSync } from "node:fs";

export class OmniWorkflowRunner {
  constructor(options = {}) {
    this.context = options.context || {};
    this.customActionHandlers = options.actionHandlers || {};
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

    // Default Action Handlers for OmniCommerce Workflow Actions
    let result;
    switch (action) {
      case "customer-data.provisionCompositeTenantEmulation":
        result = { status: "provisioned", tenantId: params.tenantId || "omni-test-tenant", touchedStores: 5 };
        break;
      case "customer-data.replayScenario":
        result = { status: "replayed", eventsCount: params.eventsCount || 10, traceId: `trace-${Date.now()}` };
        break;
      case "mongodb.checkStock":
        result = { inStock: true, sku: params.sku || input.sku || "SKU-OMNI-4K-TV", quantity: params.quantity || input.quantity || 1 };
        break;
      case "postgresql.recordSettlement":
        result = { status: "settled", orderId: params.orderId || input.orderId || `ord-${Date.now()}`, settledAt: new Date().toISOString() };
        break;
      case "kafka.producePaymentCdc":
        result = { status: "emitted", topic: "omnicommerce.payment-cdc", partition: 0 };
        break;
      case "agentmail.sendReceipt":
        result = { status: "sent", recipient: params.email || "customer@example.com" };
        break;
      case "git.createBranch":
        result = { branch: params.branchName || `fix/omni-checkout-${Date.now()}`, base: "main" };
        break;
      case "test.execute":
        result = { passed: true, totalTests: params.totalTests || 5, failedTests: 0 };
        break;
      case "github.createPullRequest":
        result = { prNumber: 42, url: "https://github.com/omni-test-tenant/omni-commerce/pull/42" };
        break;
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
