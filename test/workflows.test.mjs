import assert from "node:assert/strict";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { Client as PgClient } from "pg";
import { MongoClient } from "mongodb";
import { OmniWorkflowRunner } from "../src/omni-runner.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));

test("OmniWorkflowRunner parses and executes order-settlement workflow against live database drivers", async () => {
  const pgUrl = process.env.CDW_TEST_POSTGRES_URL || "postgresql://postgres:cdw-ci-disposable-only@127.0.0.1:5432/postgres";
  const mongoUrl = process.env.CDW_TEST_MONGO_URL || "mongodb://127.0.0.1:27017/omnicommerce_workflows";

  const pg = new PgClient({ connectionString: pgUrl, connectionTimeoutMillis: 2000 });
  await pg.connect();
  await pg.query(`
    CREATE TABLE IF NOT EXISTS payments (
      id TEXT PRIMARY KEY,
      order_id TEXT NOT NULL,
      amount_cents INTEGER NOT NULL,
      status TEXT NOT NULL
    );
  `);

  const mongo = new MongoClient(mongoUrl, { serverSelectionTimeoutMS: 2000 });
  await mongo.connect();
  const mongoDb = mongo.db();
  await mongoDb.collection("product_catalogs").insertOne({
    sku: "SKU-OMNI-4K-TV",
    stockQuantity: 10,
    priceCents: 14999
  });

  const kafkaMock = {
    send: async () => {}
  };

  try {
    const runner = new OmniWorkflowRunner({
      stores: {
        mongodb: mongoDb,
        postgres: pg,
        kafka: kafkaMock
      }
    });

    const workflowPath = join(__dirname, "../workflows/order-settlement.yaml");
    const result = await runner.runWorkflow(workflowPath, {
      orderId: "ord-settle-live-101",
      sku: "SKU-OMNI-4K-TV",
      amountCents: 14999
    });

    assert.equal(result.success, true);
    assert.equal(result.workflowName, "order-settlement");
    assert.equal(result.executedSteps.length, 4);
    assert.equal(result.output.inStock, true);
    assert.equal(result.output.status, "sent");

    // Verify row was inserted into real PostgreSQL
    const pgRes = await pg.query("SELECT * FROM payments WHERE order_id = $1", ["ord-settle-live-101"]);
    assert.equal(pgRes.rowCount, 1);
    assert.equal(pgRes.rows[0].status, "settled");
  } finally {
    await pg.query("DROP TABLE IF EXISTS payments CASCADE").catch(() => {});
    await pg.end().catch(() => {});
    await mongoDb.collection("product_catalogs").drop().catch(() => {});
    await mongo.close().catch(() => {});
  }
});

test("OmniWorkflowRunner executes automated-bug-repro pipeline steps end-to-end", async () => {
  const runner = new OmniWorkflowRunner();
  const workflowPath = join(__dirname, "../workflows/automated-bug-repro.yaml");
  const result = await runner.runWorkflow(workflowPath, {
    tenantId: "omni-test-tenant",
    seed: "workflow-repro-seed-1",
    issueId: "ROCK-999"
  });

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
