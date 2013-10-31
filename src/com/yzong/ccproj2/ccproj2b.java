package com.yzong.ccproj2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Placement;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.ConfigureHealthCheckRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.DeleteLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.HealthCheck;
import com.amazonaws.services.elasticloadbalancing.model.Instance;
import com.amazonaws.services.elasticloadbalancing.model.Listener;
import com.amazonaws.services.elasticloadbalancing.model.RegisterInstancesWithLoadBalancerRequest;

public class ccproj2b {

  /* Obtain the public DNS of the ec2 instance with given ID. */
  public static String instanceDNS(AmazonEC2Client ec2, String ID) {
    DescribeInstancesRequest request = new DescribeInstancesRequest().withInstanceIds(ID);
    DescribeInstancesResult result = ec2.describeInstances(request);
    return result.getReservations().get(0).getInstances().get(0).getPublicDnsName();
  }

  /* Given EC2 Client info, image ID, and instance type, reserve an instance. */
  static String runInstance(AmazonEC2Client ec2, String imgID, String instanceType,
      String availabilityZone) throws InterruptedException {
    // Formulate the Run Instance request.
    RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
    Placement placement = new Placement().withAvailabilityZone(availabilityZone);
    runInstancesRequest.withImageId(imgID).withMonitoring(true).withInstanceType(instanceType)
        .withMinCount(1).withMaxCount(1).withKeyName("jimmy@thinkpad t430")
        .withSecurityGroups("All").setPlacement(placement);
    // Launch the instance.
    RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);

    // To return the Instance ID of the launched instance.
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
    System.out.println("Waiting 60s for server to complete setup...");
    Thread.sleep(60000);
    return instanceID;
  }

  /**
   * @param args
   * @throws IOException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    // Load the Property File with AWS Credentials.
    Properties properties = new Properties();
    properties.load(ccproj2b.class.getResourceAsStream("/AwsCredentials.properties"));
    BasicAWSCredentials bawsc =
        new BasicAWSCredentials(properties.getProperty("accessKey"),
            properties.getProperty("secretKey"));
    // Create AWS and ELB client.
    AmazonEC2Client ec2 = new AmazonEC2Client(bawsc);
    AmazonElasticLoadBalancingClient elb = new AmazonElasticLoadBalancingClient(bawsc);

    // Set up an elastic load balancer for EC2.
    System.out.print("Setting up the elastic load balancer...");
    elb.setEndpoint("https://elasticloadbalancing.us-east-1.amazonaws.com");
    CreateLoadBalancerRequest elbRequest = new CreateLoadBalancerRequest();
    elbRequest.setLoadBalancerName("proj2b");
    List<Listener> listeners = new ArrayList<Listener>(2);
    listeners.add(new Listener("HTTP", 80, 80));
    listeners.add(new Listener("HTTP", 8080, 8080));
    elbRequest.withAvailabilityZones("us-east-1a").withListeners(listeners);

    // Request the elastic load balancer, store the DNS.
    CreateLoadBalancerResult elbResult = elb.createLoadBalancer(elbRequest);
    String loadBalancerDNS = elbResult.getDNSName();
    // Set up the Health Checker for the elastic load balancer.
    HealthCheck healthCheck = new HealthCheck();
    healthCheck.withInterval(30).withHealthyThreshold(9).withUnhealthyThreshold(2)
        .withTarget("HTTP:8080/upload").withTimeout(5);
    ConfigureHealthCheckRequest elbHealthCheckRequest =
        new ConfigureHealthCheckRequest("proj2b", healthCheck);
    elb.configureHealthCheck(elbHealthCheckRequest);
    System.out.println(" Done! (" + loadBalancerDNS + ")");

    // Initialize benchmark variables.
    float procFreq = 0; // # of requests/second.
    int instancesCount = 0; // # of worker instances.
    List<String> instanceIDs = new ArrayList<String>(); // Instance IDs
    List<Instance> elbInstances = new ArrayList<Instance>(); // Instances

    // Main benchmark loop.
    while (procFreq < 1500) {
      String newInstanceID = runInstance(ec2, "ami-700e4a19", "m1.small", "us-east-1a");
      instanceIDs.add(newInstanceID);
      elbInstances.add(new Instance(newInstanceID));
      instancesCount++;
      System.out.println("No. " + instancesCount + " instance " + newInstanceID
          + " has been reserved.");

      // Add the instance to ELB.
      RegisterInstancesWithLoadBalancerRequest regInstancesRequest =
          new RegisterInstancesWithLoadBalancerRequest();
      regInstancesRequest.setInstances(elbInstances);
      regInstancesRequest.setLoadBalancerName("proj2b");
      elb.registerInstancesWithLoadBalancer(regInstancesRequest);
      System.out.println("Waiting 600 seconds for instances to be registered on ELB...");
      Thread.sleep(600000);

      // Run the benchmark.
      System.out.println("Running tests on " + newInstanceID + "...");
      String scriptName = "./apache_bench.sh";
      String imgName = "sample.jpg";
      String totalReqCount = "100000";
      String parallelCount = "100";
      String logPrefix = "log-" + instancesCount;
      String infoPath = "output-" + instancesCount;
      File infoFile = new File(infoPath);
      Process childProcess = null;
      ProcessBuilder pb =
          new ProcessBuilder(scriptName, imgName, totalReqCount, parallelCount, loadBalancerDNS,
              logPrefix);
      pb.directory(new File("/home/ubuntu/benchmark"));
      pb.redirectOutput(infoFile);
      childProcess = pb.start();
      int exitValue = childProcess.waitFor();
      System.out.println("Completed! Exit code: " + exitValue);

      // Parse the output.
      System.out.println("Parsing the output...");
      FileInputStream parsedFile = new FileInputStream("/home/ubuntu/benchmark/" + infoPath);
      BufferedReader fileReader = new BufferedReader(new InputStreamReader(parsedFile));
      String benchmarkLine = null;
      while ((benchmarkLine = fileReader.readLine()) != null) {
        if (benchmarkLine.startsWith("Requests per second:")) {
          break;
        }
      }
      fileReader.close();
      String benchmarkVal;
      if (benchmarkLine == null) {
        benchmarkVal = "-1";
      } else {
        benchmarkVal = benchmarkLine.substring(24, benchmarkLine.length() - 15);
      }
      System.out.println("No. of connections/sec is: " + benchmarkVal);
      procFreq = Float.parseFloat(benchmarkVal);
      if (procFreq < 1500) {
        System.out.println("The throughput is not enough...");
        System.out.println("--------------------------------------\n");
      }
    }

    // Output the result, terminate the instances and ELB.
    System.out.println("Benchmark complete! Minimum number of instances required: "
        + instancesCount + ".");
    System.out.println("Terminating all worker instances...");
    TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest();
    terminateInstancesRequest.withInstanceIds(instanceIDs);
    ec2.terminateInstances(terminateInstancesRequest);
    System.out.println("Terminating the Load Balancer...");
    DeleteLoadBalancerRequest deleteELBRequest = new DeleteLoadBalancerRequest("proj2b");
    elb.deleteLoadBalancer(deleteELBRequest);
    System.out.println("Clean-up complete. Bye!");
    return;
  }

}
