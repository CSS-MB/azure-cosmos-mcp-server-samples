#!/usr/bin/env node
import { CosmosClient } from "@azure/cosmos";
import * as dotenv from "dotenv";
import { fileURLToPath } from "url";
import { dirname, resolve } from "path";

function loadEnv() {
  dotenv.config();
  try {
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = dirname(__filename);
    const parentEnvPath = resolve(__dirname, "..", ".env");
    dotenv.config({ path: parentEnvPath });
  } catch {}
}

loadEnv();

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

async function main() {
  const endpoint = requireEnv("COSMOSDB_URI");
  const key = requireEnv("COSMOSDB_KEY");
  const databaseId = requireEnv("COSMOS_DATABASE_ID");
  const containerId = requireEnv("COSMOS_CONTAINER_ID");

  const client = new CosmosClient({ endpoint, key });
  const container = client.database(databaseId).container(containerId);

  // Try to detect the partition key path
  let pkPath: string | undefined;
  try {
    const { resource } = await container.read();
    pkPath = (resource as any)?.partitionKey?.paths?.[0];
  } catch {}

  const items = [
    {
      id: "item-001",
      type: "document",
      status: "active",
      category: "test",
      priority: "high",
      value: 100,
      created: "2025-08-15T00:00:00Z",
      tags: ["sample", "document", "test"],
    },
    {
      id: "item-002",
      type: "record",
      status: "pending",
      category: "production",
      priority: "medium",
      value: 250,
      created: "2025-08-15T01:00:00Z",
      tags: ["record", "processing", "queue"],
    },
    {
      id: "item-003",
      type: "configuration",
      status: "active",
      category: "system",
      priority: "low",
      value: 75,
      created: "2025-08-15T02:00:00Z",
      tags: ["config", "settings", "system"],
    },
    {
      id: "item-004",
      type: "log",
      status: "archived",
      category: "audit",
      priority: "high",
      value: 150,
      created: "2025-08-15T03:00:00Z",
      tags: ["log", "audit", "security"],
    },
    {
      id: "item-005",
      type: "report",
      status: "active",
      category: "analytics",
      priority: "medium",
      value: 320,
      created: "2025-08-15T04:00:00Z",
      tags: ["report", "analytics", "business"],
    },
  ];

  const insertedIds: string[] = [];
  for (const raw of items) {
    const item = { ...raw } as Record<string, any>;
    if (pkPath) {
      const keyPath = pkPath.replace(/^\//, "");
      if (!(keyPath in item)) {
        item[keyPath] = keyPath === "id" ? item.id : "seed";
      }
    }
    const { resource } = await container.items.upsert(item);
    insertedIds.push((resource as any)?.id ?? item.id);
  }

  // Fetch up to 5 back to confirm
  const query = {
    query:
      "SELECT TOP 5 c.id, c['type'] AS type, c['status'] AS status, c['category'] AS category, c['priority'] AS priority, c['value'] AS val, c['created'] AS created, c['tags'] AS tags FROM c ORDER BY c.id",
  };
  const { resources } = await container.items.query(query).fetchAll();

  console.log(
    JSON.stringify({ seeded: insertedIds, items: resources }, null, 2)
  );
}

main().catch((err) => {
  console.error("Seed failed:", (err as any)?.message ?? err);
  process.exit(1);
});
