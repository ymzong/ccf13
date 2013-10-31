package com.yzong.ccproj3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import static org.apache.commons.io.FileUtils.copyURLToFile;

public class ccproj3d {

  static AmazonDynamoDBClient DDBClient;


  /**
   * @param args
   * @throws IOException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    initClient();
    createTable("proj3d");
    populateTable("proj3d", new URL("https://s3.amazonaws.com/15-319-s13/proj3/caltech-256.csv"));
    return;
  }

  /* Create a DynamoDB client using the given credentials. */
  public static void initClient() throws IOException {
    // Load the Property File with AWS Credentials.
    Properties properties = new Properties();
    properties.load(ccproj3d.class.getResourceAsStream("/AwsCredentials.properties"));
    BasicAWSCredentials bawsc =
        new BasicAWSCredentials(properties.getProperty("accessKey"),
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
    attributeDefinitions.add(new AttributeDefinition().withAttributeName("Category")
        .withAttributeType("S"));
    attributeDefinitions.add(new AttributeDefinition().withAttributeName("Picture")
        .withAttributeType("N"));
    // Specify the key schemas.
    List<KeySchemaElement> keySchemas = new ArrayList<KeySchemaElement>();
    keySchemas.add(new KeySchemaElement().withAttributeName("Category").withKeyType(KeyType.HASH));
    keySchemas.add(new KeySchemaElement().withAttributeName("Picture").withKeyType(KeyType.RANGE));
    // Generate the final request.
    createTableRequest.withTableName(tName).withProvisionedThroughput(throughput)
        .withAttributeDefinitions(attributeDefinitions).withKeySchema(keySchemas);

    // Send the initialization request to Amazon and wait.
    CreateTableResult createTableResult = DDBClient.createTable(createTableRequest);
    TableDescription tableDescription = createTableResult.getTableDescription();
    String tableName = tableDescription.getTableName();
    String tableStatus = tableDescription.getTableStatus();
    Long tableRThroughput = tableDescription.getProvisionedThroughput().getReadCapacityUnits();
    Long tableWThroughput = tableDescription.getProvisionedThroughput().getWriteCapacityUnits();
    System.out.println("Success! Following is the new table info:");
    System.out.printf("Name: %s (%s)\nRead Throughput: %d\nWrite Throughput: %d\n", tableName,
        tableStatus, tableRThroughput, tableWThroughput);
    System.out.println("Waiting for the initialization of table... (30s)");
    Thread.sleep(30000);
    tableDescription =
        DDBClient.describeTable(new DescribeTableRequest().withTableName(tableName)).getTable();
    while (!tableDescription.getTableStatus().equals("ACTIVE")) {
      System.out.println("Waiting for 5 more seconds...");
      Thread.sleep(5000);
      tableDescription =
          DDBClient.describeTable(new DescribeTableRequest().withTableName(tableName)).getTable();
    }
    System.out.printf("Success! The table %s is ready for use.\n", tableName);
    return;
  }

  /* Populate the table with given data (from the URL of a csv file). */
  public static void populateTable(String tName, URL fileURL) throws IOException {
    // Download the file from the given URL.
    System.out.println("Fetching the .csv file from Amazon S3...");
    copyURLToFile(fileURL, new File("tmp.csv"));
    System.out.println("Done! Parsing the file and populating table...");
    // Parse the csv file and populate the table along the way.
    int lineCount = 0;
    String line;
    Map<String, AttributeValue> entry = new HashMap<String, AttributeValue>();
    BufferedReader fileReader = new BufferedReader(new FileReader("tmp.csv"));
    fileReader.readLine(); // Skip the first line (of headers).
    while ((line = fileReader.readLine()) != null) {
      String[] fields = line.split(",");
      entry.clear();
      entry.put("Category", new AttributeValue().withS(fields[0]));
      entry.put("Picture", new AttributeValue().withN(fields[1]));
      entry.put("S3URL", new AttributeValue().withS(fields[2]));
      PutItemRequest putItemRequest = new PutItemRequest().withTableName(tName).withItem(entry);
      DDBClient.putItem(putItemRequest);
      lineCount++;
      System.out.printf("Done processing %d entries...\r", lineCount);
    }
    fileReader.close();
    System.out.printf("Completed populating the table %s!\n", tName);
    return;
  }
}
