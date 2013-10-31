package com.yzong.ccproj2;

import java.util.List;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Properties;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

public class ccproj2a {

  /* Obtain the public DNS of the ec2 instance with given ID. */
  public static String instanceDNS(AmazonEC2Client ec2, String ID) {
    DescribeInstancesRequest request = new DescribeInstancesRequest().withInstanceIds(ID);
    DescribeInstancesResult result = ec2.describeInstances(request);
    return result.getReservations().get(0).getInstances().get(0).getPublicDnsName();
  }

  /**
   * @param args
   * @throws IOException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    Date startMain = new Date();
    runBenchmark("m1.small");
    runBenchmark("m1.medium");
    runBenchmark("m1.large");
    Date endMain = new Date();
    System.out.println("All benchmarks complete!");
    System.out.println("Time elapsed: " + (endMain.getTime() - startMain.getTime()) + " ms.");
    return;
  }

  /**
   * @param args
   * @throws IOException
   * @throws InterruptedException
   */
  public static void runBenchmark(String instanceType) throws IOException, InterruptedException {
    // Load the Property File with AWS Credentials.
    Properties properties = new Properties();
    properties.load(ccproj2a.class.getResourceAsStream("/AwsCredentials.properties"));
    BasicAWSCredentials bawsc =
        new BasicAWSCredentials(properties.getProperty("accessKey"),
            properties.getProperty("secretKey"));

    // Create an AWS client.
    AmazonEC2Client ec2 = new AmazonEC2Client(bawsc);

    // Create RunInstance Request.
    RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
    runInstancesRequest.withImageId("ami-700e4a19").withMonitoring(true)
        .withInstanceType(instanceType).withMinCount(1).withMaxCount(1)
        .withKeyName("jimmy@thinkpad t430").withSecurityGroups("All");

    // Launch the instance.
    RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);

    // Return the Instance ID of the launched instance.
    String instanceID = runInstancesResult.getReservation().getInstances().get(0).getInstanceId();
    System.out.println("Succeeded reserving a " + instanceType + " instance! Instance ID: "
        + instanceID);

    // Wait for server to start.
    while (instanceDNS(ec2, instanceID).length() == 0) {
      System.out.println("Server booting up -- waiting for 5 more seconds...");
      Thread.sleep(5000);
    }
    String instanceDNSName = instanceDNS(ec2, instanceID);
    System.out.println("Instance initialized. Public DNS: " + instanceDNSName);
    System.out.println("Waiting 100s for server to complete setup...");
    Thread.sleep(100000);

    // Tests be executed in the ~/benchmark directory.
    // Grasp Cloud Watch log for each period.
    System.out.println("Running tests on " + instanceType + "...");
    Date childStart = new Date();
    String scriptName = "./apache_bench.sh";
    String imgName = "sample.jpg";
    String totalReqCount = "100000";
    String parallelCount = "100";
    String logPrefix = instanceType + ".log";
    String infoPrefix = instanceType + ".bm";
    int totalRounds = 12;
    for (int i = 0; i < totalRounds; i++) {
      System.out.println("Benchmarking Round " + (i + 1) + " out of " + totalRounds + ".");
      Process childProcess = null;
      ProcessBuilder pb =
          new ProcessBuilder(scriptName, imgName, totalReqCount, parallelCount, instanceDNSName,
              logPrefix + i);
      pb.directory(new File("/home/ubuntu/benchmark"));
      pb.redirectOutput(new File(infoPrefix + i));
      childProcess = pb.start();
      int exitValue = childProcess.waitFor();
      System.out.println("Completed! Exit code: " + exitValue);
    }

    // All tests have completed! Get CPU info.
    System.out.println("\nAll tests completed.");
    Date childEnd = new Date();
    System.out.println("Pulling the CPU log...");
    AmazonCloudWatchClient cloudWatchClient = new AmazonCloudWatchClient(bawsc);
    GetMetricStatisticsRequest getMetricStatisticsRequest = new GetMetricStatisticsRequest();
    getMetricStatisticsRequest.withStartTime(childStart).withEndTime(childEnd)
        .withMetricName("CPUUtilization").withPeriod(60).withNamespace("AWS/EC2")
        .withUnit("Percent").withStatistics("Average", "Maximum", "Minimum", "SampleCount", "Sum");
    GetMetricStatisticsResult getMetricStatResponse =
        cloudWatchClient.getMetricStatistics(getMetricStatisticsRequest);
    // Obtain, sort and output the metric data points.
    List<Datapoint> dataPoints = getMetricStatResponse.getDatapoints();
    Collections.sort(dataPoints, new Comparator<Datapoint>() {
      public int compare(Datapoint dp1, Datapoint dp2) {
        return (int) (dp1.getTimestamp().getTime() - dp2.getTimestamp().getTime());
      }
    });
    for (Datapoint dp : dataPoints) {
      System.out.println(dp);
    }
    // Terminate the instance.
    System.out.print("Benchmark complete for " + instanceType + ". Terminating instance...");
    TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest();
    terminateRequest.withInstanceIds(instanceID);
    ec2.terminateInstances(terminateRequest);
    System.out.println("Done!");
    System.out.println("--------\n");
  }
}
