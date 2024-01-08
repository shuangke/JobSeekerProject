package org.example;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.*;


import java.io.File;
import java.io.IOException;
import java.util.*;

public class Main {

    private static final Region region = Region.US_WEST_2;
    private static final CredentialProperties credentialProperty = new CredentialProperties();

    private static DynamoDbClient ddbClient = null;

    private static final String tableName = "Jobs";
    public static void main(String[] args) {
        handler();
    }

    public static void handler() {
        createDDBClient();
        String filePath = "/Users/shuangke/Desktop/code/projects/JobSeekerProject/JobSeeker/src/main/java/org/example/resources/jobsInfo.json";
        insertItemsByUsingFile(filePath);
        ddbClient.close();
    }

    public static void createDDBClient() {
        // Create AWS credentials object with the provided access and secret keys
        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(CredentialProperties.accessKey, CredentialProperties.secretKey);
        ddbClient = DynamoDbClient.builder()
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                .build();
    }
    public static void updateItemByJobId(String jobId, String newJobTitle, String newJobDescription) {
        System.out.println("executing updateItemByPrimaryKey......");

        // Fetch the existing item from DynamoDB
        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName(tableName)
                .key(Map.of("jobId", AttributeValue.builder().s(jobId).build()))
                .build();

        GetItemResponse getItemResponse = ddbClient.getItem(getItemRequest);

        // Check if the item exists and has differences in jobTitle or jobDescription
        if (getItemResponse.hasItem()) {
            AttributeValue existingJobTitle = getItemResponse.item().get("jobTitle");
            AttributeValue existingJobDescription = getItemResponse.item().get("jobDescription");

            boolean jobTitleChanged = !existingJobTitle.s().equals(newJobTitle);
            boolean jobDescriptionChanged = !existingJobDescription.s().equals(newJobDescription);

            if (jobTitleChanged || jobDescriptionChanged) {
                UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("jobId", AttributeValue.builder().s(jobId).build()))
                        .updateExpression("SET " +
                                (jobTitleChanged ? "jobTitle = :title, " : "") +
                                (jobDescriptionChanged ? "jobDescription = :description" : ""))
                        .conditionExpression("attribute_exists(jobId) AND " +
                                "(attribute_not_exists(jobTitle) OR jobTitle <> :title) AND " +
                                "(attribute_not_exists(jobDescription) OR jobDescription <> :description)")
                        .expressionAttributeValues(Map.of(
                                ":title", AttributeValue.builder().s(newJobTitle).build(),
                                ":description", AttributeValue.builder().s(newJobDescription).build()
                        ))
                        .build();

                try {
                    ddbClient.updateItem(updateItemRequest);
                    System.out.println("Item updated successfully.");
                } catch (DynamoDbException e) {
                    System.err.println("Unable to update item: " + e.getMessage());
                }
            } else {
                System.out.println("Values are identical. No update needed.");
            }
        } else {
            System.out.println("Item not found.");
        }

    }


    public static void deleteItemByJobId(String jobId) {
        System.out.println("executing deleteItemByPrimaryKey......");
        DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(Map.of("jobId", AttributeValue.builder().s(jobId).build()))
                .build();

        // Delete the item from DynamoDB
        try {
            ddbClient.deleteItem(deleteItemRequest);
            System.out.println("Item deleted successfully. jobId: " + jobId);
        } catch (DynamoDbException e) {
            System.err.println("Unable to delete item: " + e.getMessage());
        }
    }

    private static void insertItemsByUsingFile(String filePath) {
        System.out.println("executing insertItemsByUsingFile2......");
        JsonParser parser = null;
        try {
            parser = new JsonFactory()
                    .createParser(new File(filePath));
            JsonNode rootNode = new ObjectMapper().readTree(parser);
            Iterator<JsonNode> iter = rootNode.iterator();
            ObjectNode currentNode;
            while (iter.hasNext()) {
                currentNode = (ObjectNode) iter.next();
                String jobId = currentNode.path("jobId").asText();
                String jobTitile = currentNode.path("jobTitle").asText();
                String jobDescription = currentNode.path("jobDescription").asText();

                try {
                    insertItem(jobId, jobTitile, jobDescription);
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
    public static void insertItem(String jobId, String jobTitle, String jobDescription) {
        System.out.println("insertItem........");
        // Create a map of attribute values for the item
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put("jobId", AttributeValue.builder().s(jobId).build());
        itemValues.put("jobTitle", AttributeValue.builder().s(jobTitle).build());
        itemValues.put("jobDescription", AttributeValue.builder().s(jobDescription).build());

        // Create a PutItemRequest
        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(itemValues)
                .build();

        // Insert the item into DynamoDB
        try {
            ddbClient.putItem(putItemRequest);
            System.out.println("Successful load: " + jobId + " " + jobTitle + " " + jobDescription);
        } catch (DynamoDbException e) {
            System.err.println("Unable to insert item: " + e.getMessage());
        }
    }
}