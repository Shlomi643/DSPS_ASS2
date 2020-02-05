import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import static utils.Utils.*;

public class Main {

    public static void main(String[] args) {

        AWSCredentialsProvider credentials = DefaultAWSCredentialsProviderChain.getInstance();

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard().withRegion(REGION).build();

        StepConfig step1 = new StepConfig()
                .withName("step")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(new HadoopJarStepConfig()
                        .withJar(JAR_URL)
                        .withMainClass("mr.MapReduce")
                        .withArgs(CORPUS, OUTPUT_URL));

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(NUM_OF_INSTANCES)
                .withMasterInstanceType(INSTANCE_TYPE)
                .withSlaveInstanceType(INSTANCE_TYPE)
                .withHadoopVersion(HADOOP_VERSION)
                .withEc2KeyName(EC2_KEY_NAME)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withEc2SubnetId(SUBNET_ID)
                .withPlacement(new PlacementType(REGION));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("starter")
                .withInstances(instances)
                .withSteps(step1)
                .withLogUri(BUCKET_LOGS);

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
}
