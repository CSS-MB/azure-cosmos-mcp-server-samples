package com.example.mcp;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;

public class CosmosDBMcpServer {

    private static final Logger logger = LoggerFactory.getLogger(CosmosDBMcpServer.class);

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        var transport = new StdioServerTransportProvider(mapper);

        var capabilities = McpSchema.ServerCapabilities.builder()
                .resources(false, true)
                .tools(true)
                .logging()
                .build();

        McpSyncServer server = McpServer.sync(transport)
                .serverInfo("cosmosdb-mcp-server", "1.0.0")
                .capabilities(capabilities)
                .build();

        String endpoint = System.getenv("COSMOSDB_URI");
        String key = System.getenv("COSMOSDB_KEY");
        String databaseId = System.getenv("COSMOS_DATABASE_ID");

        if (endpoint == null || endpoint.isBlank() || databaseId == null || databaseId.isBlank()) {
            logger.error("Missing required environment variables: COSMOSDB_URI and/or COSMOS_DATABASE_ID.");
            System.err.println("Missing required environment variables: COSMOSDB_URI and/or COSMOS_DATABASE_ID.");
            System.exit(1);
        }

        CosmosClientBuilder clientBuilder = new CosmosClientBuilder()
                .endpoint(endpoint)
                .consistencyLevel(ConsistencyLevel.EVENTUAL);

        if (key != null && !key.isBlank()) {
            clientBuilder.key(key);
            logger.info("Using key-based Cosmos DB authentication.");
        } else {
            clientBuilder.credential(new DefaultAzureCredentialBuilder().build());
            logger.info("Using DefaultAzureCredential for Cosmos DB authentication.");
        }

        CosmosAsyncClient client = clientBuilder.buildAsyncClient();

        // Shutdown hook to close the client
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Cosmos client...");
            client.close();
        }));

        var getItemTool = new McpServerFeatures.SyncToolSpecification(
                new McpSchema.Tool("get_item", "Retrieves an item from a Cosmos DB container by ID",
                        "{\"type\": \"object\", \"properties\": {\"containerName\": {\"type\": \"string\"}, \"id\": {\"type\": \"string\"}}, \"required\": [\"containerName\", \"id\"]}"),
                (exchange, argsMap) -> {
                    try {
                        String containerName = requireStringArg(argsMap, "containerName");
                        String id = requireStringArg(argsMap, "id");
                        String pkValue = argsMap.getOrDefault("partitionKey", id).toString();

                        CosmosAsyncContainer container = client.getDatabase(databaseId).getContainer(containerName);
                        CosmosItemResponse<ObjectNode> response = container
                                .readItem(id, new PartitionKey(pkValue), ObjectNode.class)
                                .block();
                        if (response == null || response.getItem() == null) {
                            logger.warn("Item not found: container={}, id={}", containerName, id);
                            return new McpSchema.CallToolResult("Error: Item not found", true);
                        }
                        String json = mapper.writeValueAsString(response.getItem());
                        return new McpSchema.CallToolResult(json, false);
                    } catch (CosmosException ce) {
                        if (ce.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                            logger.warn("Item not found: container={}, id={}", argsMap.get("containerName"), argsMap.get("id"));
                            return new McpSchema.CallToolResult("Error: Item not found", true);
                        }
                        logger.warn("Cosmos DB error in get_item: statusCode={}, message={}", ce.getStatusCode(), ce.getMessage());
                        return new McpSchema.CallToolResult("Error: " + ce.getMessage(), true);
                    } catch (JsonProcessingException jpe) {
                        logger.warn("Serialization error in get_item: {}", jpe.getOriginalMessage());
                        return new McpSchema.CallToolResult("Error: Failed to serialize item", true);
                    } catch (IllegalArgumentException iae) {
                        logger.warn("Invalid arguments for get_item: {}", iae.getMessage());
                        return new McpSchema.CallToolResult("Error: " + iae.getMessage(), true);
                    } catch (RuntimeException re) {
                        logger.warn("Error in get_item: {}", re.getMessage());
                        return new McpSchema.CallToolResult("Error: " + re.getMessage(), true);
                    }
                });

        var putItemTool = new McpServerFeatures.SyncToolSpecification(
                new McpSchema.Tool("put_item", "Inserts or replaces an item in a Cosmos DB container",
                        "{\"type\": \"object\", \"properties\": {\"containerName\": {\"type\": \"string\"}, \"item\": {\"type\": \"object\"}}, \"required\": [\"containerName\", \"item\"]}"),
                (exchange, argsMap) -> {
                    try {
                        String containerName = requireStringArg(argsMap, "containerName");
                        Object rawItem = Objects.requireNonNull(argsMap.get("item"), "item is required");
                        ObjectNode itemNode = mapper.convertValue(rawItem, ObjectNode.class);
                        if (!itemNode.hasNonNull("id")) {
                            itemNode.put("id", UUID.randomUUID().toString());
                        }
                        CosmosAsyncContainer container = client.getDatabase(databaseId).getContainer(containerName);
                        container.upsertItem(itemNode).block();
                        try {
                            return new McpSchema.CallToolResult(mapper.writeValueAsString(itemNode), false);
                        } catch (JsonProcessingException jpe) {
                            logger.warn("Serialization error in put_item: {}", jpe.getOriginalMessage());
                            return new McpSchema.CallToolResult("Error: Failed to serialize item", true);
                        }
                    } catch (CosmosException ce) {
                        logger.warn("Cosmos DB error in put_item: statusCode={}, message={}", ce.getStatusCode(), ce.getMessage());
                        return new McpSchema.CallToolResult("Error: " + ce.getMessage(), true);
                    } catch (IllegalArgumentException | NullPointerException iae) {
                        logger.warn("Invalid arguments for put_item: {}", iae.getMessage());
                        return new McpSchema.CallToolResult("Error: " + iae.getMessage(), true);
                    } catch (RuntimeException re) {
                        logger.warn("Error in put_item: {}", re.getMessage());
                        return new McpSchema.CallToolResult("Error: " + re.getMessage(), true);
                    }
                });

        var updateItemTool = new McpServerFeatures.SyncToolSpecification(
                new McpSchema.Tool("update_item", "Updates fields in an existing Cosmos DB item",
                        "{\"type\": \"object\", \"properties\": {\"containerName\": {\"type\": \"string\"}, \"id\": {\"type\": \"string\"}, \"updates\": {\"type\": \"object\"}}, \"required\": [\"containerName\", \"id\", \"updates\"]}"),
                (exchange, argsMap) -> {
                    try {
                        String containerName = requireStringArg(argsMap, "containerName");
                        String id = requireStringArg(argsMap, "id");
                        String pkValue = argsMap.getOrDefault("partitionKey", id).toString();
                        Object rawUpdates = Objects.requireNonNull(argsMap.get("updates"), "updates is required");
                        ObjectNode updates = mapper.convertValue(rawUpdates, ObjectNode.class);

                        CosmosAsyncContainer container = client.getDatabase(databaseId).getContainer(containerName);
                        CosmosItemResponse<ObjectNode> response = container
                                .readItem(id, new PartitionKey(pkValue), ObjectNode.class)
                                .block();
                        if (response == null || response.getItem() == null) {
                            logger.warn("Item not found for update: container={}, id={}", containerName, id);
                            return new McpSchema.CallToolResult("Error: Item not found", true);
                        }
                        ObjectNode current = response.getItem().deepCopy();
                        deepMerge(current, updates);
                        container.replaceItem(current, id, new PartitionKey(pkValue)).block();
                        return new McpSchema.CallToolResult(mapper.writeValueAsString(current), false);
                    } catch (CosmosException ce) {
                        if (ce.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                            logger.warn("Item not found for update: container={}, id={}", argsMap.get("containerName"), argsMap.get("id"));
                            return new McpSchema.CallToolResult("Error: Item not found", true);
                        }
                        logger.warn("Cosmos DB error in update_item: statusCode={}, message={}", ce.getStatusCode(), ce.getMessage());
                        return new McpSchema.CallToolResult("Error: " + ce.getMessage(), true);
                    } catch (IllegalArgumentException | NullPointerException iae) {
                        logger.warn("Invalid arguments for update_item: {}", iae.getMessage());
                        return new McpSchema.CallToolResult("Error: " + iae.getMessage(), true);
                    } catch (JsonProcessingException jpe) {
                        logger.warn("Serialization error in update_item: {}", jpe.getOriginalMessage());
                        return new McpSchema.CallToolResult("Error: Failed to serialize item", true);
                    } catch (RuntimeException re) {
                        logger.warn("Error in update_item: {}", re.getMessage());
                        return new McpSchema.CallToolResult("Error: " + re.getMessage(), true);
                    }
                });

        var queryContainerTool = new McpServerFeatures.SyncToolSpecification(
                new McpSchema.Tool("query_container", "Runs a SQL query against a Cosmos DB container",
                        "{\"type\": \"object\", \"properties\": {\"containerName\": {\"type\": \"string\"}, \"query\": {\"type\": \"string\"}}, \"required\": [\"containerName\", \"query\"]}"),
                (exchange, argsMap) -> {
                    try {
                        logger.info("Received query_container request with args: {}", argsMap);
                        String containerName = requireStringArg(argsMap, "containerName");
                        String query = requireStringArg(argsMap, "query");
                        CosmosAsyncContainer container = client.getDatabase(databaseId).getContainer(containerName);

                        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
                        List<Object> list = new ArrayList<>();
                        container.queryItems(query, options, Object.class)
                                .toIterable()
                                .forEach(list::add);
                        String json = mapper.writeValueAsString(list);
                        return new McpSchema.CallToolResult(json, false);
                    } catch (CosmosException ce) {
                        logger.warn("Cosmos DB error in query_container: statusCode={}, message={}", ce.getStatusCode(), ce.getMessage());
                        return new McpSchema.CallToolResult("Error: " + ce.getMessage(), true);
                    } catch (IllegalArgumentException iae) {
                        logger.warn("Invalid arguments for query_container: {}", iae.getMessage());
                        return new McpSchema.CallToolResult("Error: " + iae.getMessage(), true);
                    } catch (JsonProcessingException jpe) {
                        logger.warn("Serialization error in query_container: {}", jpe.getOriginalMessage());
                        return new McpSchema.CallToolResult("Error: Failed to serialize query results", true);
                    } catch (RuntimeException re) {
                        logger.warn("Error in query_container: {}", re.getMessage());
                        return new McpSchema.CallToolResult("Error: " + re.getMessage(), true);
                    }
                });

        server.addTool(getItemTool);
        server.addTool(putItemTool);
        server.addTool(updateItemTool);
        server.addTool(queryContainerTool);

        logger.info("CosmosDB MCP server running...");
    }

    private static String requireStringArg(Map<String, Object> map, String key) {
        Object v = map.get(key);
        if (v == null) {
            throw new IllegalArgumentException(key + " is required");
        }
        if (!(v instanceof String)) {
            throw new IllegalArgumentException(key + " must be a string");
        }
        String s = (String) v;
        if (s.isBlank()) {
            throw new IllegalArgumentException(key + " cannot be blank");
        }
        return s;
    }

    // Deep merge JSON (updates into target)
    private static void deepMerge(ObjectNode target, ObjectNode updates) {
        updates.fields().forEachRemaining(e -> {
            String field = e.getKey();
            JsonNode value = e.getValue();
            if (value.isObject() && target.get(field) != null && target.get(field).isObject()) {
                deepMerge((ObjectNode) target.get(field), (ObjectNode) value);
            } else {
                target.set(field, value);
            }
        });
    }
}
