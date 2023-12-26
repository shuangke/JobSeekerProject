package org.example;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class Main {

    private static final String region = "us-west-2";
    private static final CredentialProperties credentialProperty = new CredentialProperties();
    private static DynamoDB dynamoDB = null;

    public static void main(String[] args) {
        //create DynamoDB client
        AmazonDynamoDB ddbClient = createClient();
        DynamoDBMapper mapper = new DynamoDBMapper(ddbClient);
        dynamoDB = new DynamoDB(ddbClient);
        //create Jobs table
        String tableName = "Jobs";
        Table table = createTable(tableName);
        insertItemsByUsingFile(table);
    }

    private static AmazonDynamoDB createClient() {
        AWSCredentialsProvider creds = new AWSStaticCredentialsProvider(new BasicAWSCredentials(credentialProperty.accessKey, credentialProperty.secretKey));
        AmazonDynamoDB ddbClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(creds)
                .withRegion(region)
                .build();
        return ddbClient;
    }

    private static void insertItemsByUsingFile(Table table) {
        JsonParser parser = null;
        try {
            parser = new JsonFactory()
                    .createParser(new File("src/jobsInfo.json"));
            JsonNode rootNode = new ObjectMapper().readTree(parser);
            Iterator<JsonNode> iter = rootNode.iterator();
            ObjectNode currentNode;

            while (iter.hasNext()) {
                currentNode = (ObjectNode) iter.next();
                int jobId = currentNode.path("jobId").asInt();
                String jobTitile = currentNode.path("jobTitle").asText();
                String jobDescription = currentNode.path("jobDescription").asText();

                try {
                    insertItem(jobId, jobTitile, jobDescription, table);
                    System.out.println("Successful load: " + jobId + " " + jobTitile + " " + jobDescription);
                } catch (Exception e) {
                    System.err.println("Cannot add product: " + jobId + " " + jobTitile + " " + jobDescription);
                    System.err.println(e.getMessage());
                    break;
                }
            }
            parser.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }


    }
    private static void insertItem(int jobId, String jobTitle, String jobDescription, Table table) {
        try {
            Item item = new Item()
                    .withNumber("jobId", jobId)
                    .withString("jobTitle", jobTitle)
                    .withString("jobDescription", jobDescription);
            table.putItem(item);
            System.out.println("Item created");
        } catch (Exception e) {
            System.out.println("Cannot create item");
            System.out.println(e.getMessage());
        }
    }
    public static boolean isTableExist(String tableName) {
        try {
            TableDescription tableDescription = dynamoDB.getTable(tableName).describe();
            System.out.println("Table description: " + tableDescription.getTableStatus());
            return true;
        } catch (com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException rnfe) {
            System.out.println("Table does not exist");
        }
        return false;

    }

    private static Table createTable(String tableName) {
        try {
            if (isTableExist(tableName)) {
                System.out.println("the table is already exists");
                return dynamoDB.getTable(tableName);
            }
            System.out.println("Creating the "  + tableName + " table, wait...");
            Table table = dynamoDB.createTable (tableName,
                    Arrays.asList (
                            new KeySchemaElement("jobId", KeyType.HASH) // the partition key
                    ),
                    Arrays.asList (
                            new AttributeDefinition("jobId", ScalarAttributeType.N)
                    ),
                    new ProvisionedThroughput(5L, 5L)
            );
            table.waitForActive();
            System.out.println("Table created successfully.  Status: " +
                    table.getDescription().getTableStatus());
            return table;
        } catch (Exception e) {
            System.err.println("Cannot create the table: ");
            System.err.println(e.getMessage());
        }
        return null;
    }
}