#!/usr/bin/env node
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  Tool,
} from "@modelcontextprotocol/sdk/types.js";
import { CosmosClient } from "@azure/cosmos";
import * as dotenv from "dotenv";
import { fileURLToPath } from "url";
import { dirname, resolve } from "path";

// Load environment variables from .env in both src and dist contexts
function loadEnv() {
  // First, try default lookup (current working directory)
  dotenv.config();

  // Then, also try a .env one directory up from the compiled file (useful when running from dist)
  try {
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = dirname(__filename);
    const parentEnvPath = resolve(__dirname, "..", ".env");
    dotenv.config({ path: parentEnvPath });
  } catch {
    // best-effort; ignore if path resolution fails (e.g., non-Node environment)
  }
}

loadEnv();


// Validate and access environment configuration
const requiredEnv = ["COSMOSDB_URI", "COSMOSDB_KEY", "COSMOS_DATABASE_ID"] as const;
const missing = requiredEnv.filter((k) => !process.env[k]);
if (missing.length) {
  console.error(
    `Missing required environment variables: ${missing.join(", ")}.\n` +
      "Create a .env file in the 'javascript' folder with:\n" +
      "COSMOSDB_URI=\nCOSMOSDB_KEY=\nCOSMOS_DATABASE_ID=\nCOSMOS_CONTAINER_ID= (optional if you will pass containerName in each tool call)\n"
  );
  process.exit(1);
}

const COSMOSDB_URI = process.env.COSMOSDB_URI as string;
const COSMOSDB_KEY = process.env.COSMOSDB_KEY as string;
const databaseId = process.env.COSMOS_DATABASE_ID as string;
const defaultContainerId = process.env.COSMOS_CONTAINER_ID; // optional; can be provided per-call

// Cosmos DB client initialization
const cosmosClient = new CosmosClient({ endpoint: COSMOSDB_URI, key: COSMOSDB_KEY });

// Helper to resolve the container per request (falls back to default if set)
function resolveContainer(containerName?: string) {
  const name = containerName ?? defaultContainerId;
  if (!name) {
    throw new Error(
      "No container specified. Provide COSMOS_CONTAINER_ID in .env or pass 'containerName' in the tool call."
    );
  }
  return cosmosClient.database(databaseId).container(name);
}

// Tool definitions
const UPDATE_ITEM_TOOL: Tool = {
  name: "update_item",
  description: "Updates specific attributes of an item in a Azure Cosmos DB container",
  inputSchema: {
    type: "object",
    properties: {
      containerName: { type: "string", description: "Name of the container" },
      id: { type: "string", description: "ID of the item to update" },
      updates: { type: "object", description: "The updated attributes of the item" },
    },
    required: ["containerName", "id", "updates"],
  },
};

const PUT_ITEM_TOOL: Tool = {
  name: "put_item",
  description: "Inserts or replaces an item in a Azure Cosmos DB container",
  inputSchema: {
    type: "object",
    properties: {
      containerName: { type: "string", description: "Name of the  container" },
      item: { type: "object", description: "Item to insert into the container" },
    },
    required: ["containerName", "item"],
  },
};

const GET_ITEM_TOOL: Tool = {
  name: "get_item",
  description: "Retrieves an item from a Azure Cosmos DB container by its ID",
  inputSchema: {
    type: "object",
    properties: {
      containerName: { type: "string", description: "Name of the container" },
      id: { type: "string", description: "ID of the item to retrieve" },
    },
    required: ["containerName", "id"],
  },
};

const QUERY_CONTAINER_TOOL: Tool = {
  name: "query_container",
  description: "Queries an Azure Cosmos DB container using SQL-like syntax",
  inputSchema: {
    type: "object",
    properties: {
      containerName: { type: "string", description: "Name of the container" },
      query: { type: "string", description: "SQL query string" },
      parameters: { 
        type: "array", 
        description: "Query parameters",
        items: {
          type: "object",
          properties: {
            name: { type: "string", description: "Parameter name" },
            value: { type: "string", description: "Parameter value" } // Adjust type as needed
          },
          required: ["name", "value"]
        }
      },
    },
    required: ["containerName", "query", "parameters"],
  },
};

async function updateItem(params: any) {
  try {
  const { id, updates, containerName } = params;
  const container = resolveContainer(containerName);
  const { resource } = await container.item(id).read();
    
    if (!resource) {
      throw new Error("Item not found");
    }

    const updatedItem = { ...resource, ...updates };

  const { resource: updatedResource } = await container.item(id).replace(updatedItem);
    return {
      success: true,
      message: `Item updated successfully`,
      item: updatedResource,
    };
  } catch (error) {
    console.error("Error updating item:", error);
    return {
      success: false,
      message: `Failed to update item: ${error}`,
    };
  }
}

async function putItem(params: any) {
  try {
  const { item, containerName } = params;
  const container = resolveContainer(containerName);
    const { resource } = await container.items.create(item);

    return {
      success: true,
      message: `Item added successfully to container`,
      item: resource,
    };
  } catch (error) {
    console.error("Error putting item:", error);
    return {
      success: false,
      message: `Failed to put item: ${error}`,
    };
  }
}

async function getItem(params: any) {
  try {
  const { id, containerName } = params;
  const container = resolveContainer(containerName);
    const { resource } = await container.item(id).read();

    return {
      success: true,
      message: `Item retrieved successfully`,
      item: resource,
    };
  } catch (error) {
    console.error("Error getting item:", error);
    return {
      success: false,
      message: `Failed to get item: ${error}`,
    };
  }
}

async function queryContainer(params: any) {
  try {
  const { query, parameters, containerName } = params;
  const container = resolveContainer(containerName);
    const { resources } = await container.items.query({ query, parameters }).fetchAll();

    return {
      success: true,
      message: `Query executed successfully`,
      items: resources,
    };
  } catch (error) {
    console.error("Error querying container:", error);
    return {
      success: false,
      message: `Failed to query container: ${error}`,
    };
  }
}


const server = new Server(
  {
    name: "cosmosdb-mcp-server",
    version: "0.1.0",
  },
  {
    capabilities: {
      tools: {},
    },
  },
);

// Request handlers
server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [PUT_ITEM_TOOL, GET_ITEM_TOOL, QUERY_CONTAINER_TOOL, UPDATE_ITEM_TOOL],
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  try {
    let result;
    switch (name) {
      case "put_item":
        result = await putItem(args);
        break;
      case "get_item":
        result = await getItem(args);
        break;
      case "query_container":
        result = await queryContainer(args);
        break;
      case "update_item":
        result = await updateItem(args);
        break;
      default:
        return {
          content: [{ type: "text", text: `Unknown tool: ${name}` }],
          isError: true,
        };
    }

    return {
      content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
    };
  } catch (error) {
    return {
      content: [{ type: "text", text: `Error occurred: ${error}` }],
      isError: true,
    };
  }
});

// Server startup
async function runServer() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("Azure Cosmos DB Server running on stdio");
}

runServer().catch((error) => {
  console.error("Fatal error running server:", error);
  process.exit(1);
});
