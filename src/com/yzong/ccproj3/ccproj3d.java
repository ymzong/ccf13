package com.yzong.ccproj3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

public class ccproj3d {

    static AmazonDynamoDBClient DDBClient;

    /* Create a DynamoDB client using the given credentials. */
    public static void initClient() throws IOException {
        // Load the Property File with AWS Credentials.
        Properties properties = new Properties();
        properties.load(ccproj3d.class
                .getResourceAsStream("/AwsCredentials.properties"));
        BasicAWSCredentials bawsc = new BasicAWSCredentials(
                properties.getProperty("accessKey"),
                properties.getProperty("secretKey"));
        // Create DynamoDB client.
        DDBClient = new AmazonDynamoDBClient(bawsc);
        DDBClient.setEndpoint("https://dynamodb.us-east-1.amazonaws.com");
        return;
    }


    /* Create a table of given name, following the specified spec. */
    public static void createTable(String tName) throws InterruptedException {
        // Create table initialization request.
        CreateTableRequest createTableRequest = new CreateTableRequest();
        // Allocate R/W throughput of the table.
        ProvisionedThroughput throughput = new ProvisionedThroughput();
        throughput.withReadCapacityUnits(500L).withWriteCapacityUnits(200L);
        // The table has Hash Key attribute "Category" as String type, and Range
        // Key attribute "Picture" as Number type.
        List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName(
                "Category").withAttributeType("S"));
        attributeDefinitions.add(new AttributeDefinition().withAttributeName(
                "Picture").withAttributeType("N"));
        // Specify the key schemas.
        List<KeySchemaElement> keySchemas = new ArrayList<KeySchemaElement>();
        keySchemas.add(new KeySchemaElement().withAttributeName("Category")
                .withKeyType(KeyType.HASH));
        keySchemas.add(new KeySchemaElement().withAttributeName("Picture")
                .withKeyType(KeyType.RANGE));
        // Generate the final request.
        createTableRequest.withTableName(tName)
                .withProvisionedThroughput(throughput)
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(keySchemas);

        // Send the initialization request to Amazon and wait.
        CreateTableResult createTableResult = DDBClient
                .createTable(createTableRequest);
        TableDescription tableDescription = createTableResult
                .getTableDescription();
        String tableName = tableDescription.getTableName();
        String tableStatus = tableDescription.getTableStatus();
        Long tableRThroughput = tableDescription.getProvisionedThroughput()
                .getReadCapacityUnits();
        Long tableWThroughput = tableDescription.getProvisionedThroughput()
                .getWriteCapacityUnits();
        System.out.println("Success! Following is the new table info:");
        System.out.printf(
                "Name: %s (%s)\nRead Throughput: %d\nWrite Throughput: %d\n",
                tableName, tableStatus, tableRThroughput, tableWThroughput);
        System.out.println("Waiting for the initialization of table... (20s)");
        Thread.sleep(20000);
        tableDescription = DDBClient.describeTable(
                new DescribeTableRequest().withTableName(tableName)).getTable();
        while (!tableDescription.getTableStatus().equals("ACTIVE")) {
            System.out.println("Waiting for 5 more seconds...");
            Thread.sleep(5000);
            tableDescription = DDBClient.describeTable(
                    new DescribeTableRequest().withTableName(tableName))
                    .getTable();
        }
        System.out.printf("Success! The table %s is ready for use.\n",
                tableName);
        return;
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException,
            InterruptedException {
        initClient();
        createTable("proj3d");
        return;
    }
}
