package com.yzong.ccproj2;

import java.io.IOException;
import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;

public class ccproj2b {

	/* Obtain the public DNS of the ec2 instance with given ID. */
	public static String instanceDNS(AmazonEC2Client ec2, String ID) {
		DescribeInstancesRequest request = new DescribeInstancesRequest()
				.withInstanceIds(ID);
		DescribeInstancesResult result = ec2.describeInstances(request);
		return result.getReservations().get(0).getInstances().get(0)
				.getPublicDnsName();
	}

	/* Given EC2 Client info, image ID, and instance type, reserve an instance. */
	String runInstance(AmazonEC2Client ec2, String imgID, String instanceType,
			String availabilityZone) throws InterruptedException {
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
		runInstancesRequest.withImageId(imgID).withMonitoring(true)
				.withInstanceType(instanceType).withMinCount(1).withMaxCount(1)
				.withKeyName("jimmy@thinkpad t430").withSecurityGroups("All");
		// Launch the instance.
		RunInstancesResult runInstancesResult = ec2
				.runInstances(runInstancesRequest);

		// To return the Instance ID of the launched instance.
		String instanceID = runInstancesResult.getReservation().getInstances()
				.get(0).getInstanceId();
		System.out.println("Succeeded reserving a " + instanceType
				+ " instance! Instance ID: " + instanceID);

		// Wait for server to start.
		while (instanceDNS(ec2, instanceID).length() == 0) {
			System.out
					.println("Server booting up -- waiting for 5 more seconds...");
			Thread.sleep(5000);
		}
		String instanceDNSName = instanceDNS(ec2, instanceID);
		System.out.println("Instance initialized. Public DNS: "
				+ instanceDNSName);
		System.out.println("Waiting 100s for server to complete setup...");
		Thread.sleep(100000);
		return instanceID;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// Load the Property File with AWS Credentials.
		Properties properties = new Properties();
		properties.load(ccproj2a.class
				.getResourceAsStream("/AwsCredentials.properties"));
		BasicAWSCredentials bawsc = new BasicAWSCredentials(
				properties.getProperty("accessKey"),
				properties.getProperty("secretKey"));
		// Create an AWS client.
		AmazonEC2Client ec2 = new AmazonEC2Client(bawsc);

		// Set up an elastic load balancer for EC2.
		// TODO: Set up an ELB.

		int procFreq = 0; // Number of requests handled per second.
		while (procFreq < 1500) {
			// TODO: allocate new instance, add to ELB, benchmark, parse file to
			// procFreq.
		}
	}

}
