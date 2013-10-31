package com.yzong.ccproj2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyRequest;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyResult;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.ConfigureHealthCheckRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.HealthCheck;
import com.amazonaws.services.elasticloadbalancing.model.Listener;

public class ccproj2c {

  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    // Load the Property File with AWS Credentials.
    Properties properties = new Properties();
    properties.load(ccproj2c.class.getResourceAsStream("/AwsCredentials.properties"));
    BasicAWSCredentials bawsc =
        new BasicAWSCredentials(properties.getProperty("accessKey"),
            properties.getProperty("secretKey"));

    // Create ELB, Auto Scaling, and CloudWatch client.
    AmazonElasticLoadBalancingClient elb = new AmazonElasticLoadBalancingClient(bawsc);
    AmazonAutoScalingClient asc = new AmazonAutoScalingClient(bawsc);
    AmazonCloudWatchClient cwc = new AmazonCloudWatchClient(bawsc);

    // Set up an elastic load balancer for EC2.
    System.out.print("Setting up the elastic load balancer...");
    elb.setEndpoint("https://elasticloadbalancing.us-east-1.amazonaws.com");
    CreateLoadBalancerRequest elbRequest = new CreateLoadBalancerRequest();
    elbRequest.setLoadBalancerName("proj2c-lb");
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
        new ConfigureHealthCheckRequest("proj2c-lb", healthCheck);
    elb.configureHealthCheck(elbHealthCheckRequest);
    System.out.println(" Done! (" + loadBalancerDNS + ")");

    // Create a Launch Configuration for the Auto Scaling Group.
    System.out.print("Creating a Launch Config for Auto Scaling group...");
    CreateLaunchConfigurationRequest launchConfigRequest = new CreateLaunchConfigurationRequest();
    launchConfigRequest.withLaunchConfigurationName("proj2c-launchconfig")
        .withImageId("ami-2b7b2c42").withInstanceType("m1.small").withSecurityGroups("All")
        .withKeyName("jimmy@thinkpad t430");
    asc.createLaunchConfiguration(launchConfigRequest);
    System.out.println("  Done!");

    // Create an Auto Scaling Group.
    System.out.print("Initializing an Auto Scaling Group...");
    CreateAutoScalingGroupRequest scalingGroupRequest = new CreateAutoScalingGroupRequest();
    scalingGroupRequest.withAutoScalingGroupName("proj2c-autoscaling")
        .withAvailabilityZones("us-east-1a").withDesiredCapacity(2).withHealthCheckGracePeriod(60)
        .withLoadBalancerNames("proj2c-lb").withMinSize(2).withMaxSize(5)
        .withLaunchConfigurationName("proj2c-launchconfig");
    asc.createAutoScalingGroup(scalingGroupRequest);
    System.out.println("  Done!");

    // Create ScaleIn and ScaleOut policies for the Auto Scaling Group.
    // ScaleOut is for adding one more instance to the group.
    System.out.print("Creating ScaleIn and ScaleOut policies...");
    PutScalingPolicyRequest scaleOutRequest = new PutScalingPolicyRequest();
    scaleOutRequest.withScalingAdjustment(1).withAdjustmentType("ChangeInCapacity")
        .withPolicyName("ScaleOut").withAutoScalingGroupName("proj2c-autoscaling");
    // ScaleIn is for removing one instance from the group.
    PutScalingPolicyRequest scaleInRequest = new PutScalingPolicyRequest();
    scaleInRequest.withScalingAdjustment(-1).withAdjustmentType("ChangeInCapacity")
        .withPolicyName("ScaleIn").withAutoScalingGroupName("proj2c-autoscaling");
    PutScalingPolicyResult scaleOutResult = asc.putScalingPolicy(scaleOutRequest);
    PutScalingPolicyResult scaleInResult = asc.putScalingPolicy(scaleInRequest);
    // Get the ARNs for ScaleOut and ScaleIn requests.
    String scaleOutPolicyARN = scaleOutResult.getPolicyARN();
    String scaleInPolicyARN = scaleInResult.getPolicyARN();
    System.out.println("  Done!");
    System.out.println("ScaleOut Request ARN: " + scaleOutPolicyARN);
    System.out.println("ScaleIn Request ARN: " + scaleInPolicyARN);

    // Create CloudWatch alarms that invoke ScaleIn/Out policy.
    // ScaleOut policy setup.
    System.out.print("Initializing CloudWatch alarms...");
    String emailingARN = "arn:aws:sns:us-east-1:362426542109:proj2c";
    PutMetricAlarmRequest scaleOutAlarmRequest = new PutMetricAlarmRequest();
    List<String> scaleOutARNs = new ArrayList<String>();
    scaleOutARNs.add(emailingARN);
    scaleOutARNs.add(scaleOutPolicyARN);
    scaleOutAlarmRequest.withActionsEnabled(true).withAlarmName("scaleOutAlarm")
        .withComparisonOperator("GreaterThanThreshold").withMetricName("CPUUtilization")
        .withThreshold(80.0).withPeriod(60).withEvaluationPeriods(5).withNamespace("AWS/EC2")
        .withStatistic("Average").withUnit("Percent").withAlarmActions(scaleOutARNs);
    cwc.putMetricAlarm(scaleOutAlarmRequest);
    // ScaleIn policy setup.
    PutMetricAlarmRequest scaleInAlarmRequest = new PutMetricAlarmRequest();
    List<String> scaleInARNs = new ArrayList<String>();
    scaleInARNs.add(emailingARN);
    scaleInARNs.add(scaleInPolicyARN);
    scaleInAlarmRequest.withActionsEnabled(true).withAlarmName("scaleInAlarm")
        .withComparisonOperator("GreaterThanThreshold").withMetricName("CPUUtilization")
        .withThreshold(20.0).withPeriod(60).withEvaluationPeriods(5).withNamespace("AWS/EC2")
        .withStatistic("Average").withUnit("Percent").withAlarmActions(scaleInARNs);
    cwc.putMetricAlarm(scaleInAlarmRequest);
    System.out.println("  Done!\n---------------");
    System.out.println("Auto Scaling group has been set up!");
    System.out.println("Group Name: proj2c-autoscaling");
  }
}
