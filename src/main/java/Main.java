import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.hadoop.io.Text;
import utils.Utils;

import static utils.Utils.*;

public class Main {

    public static void main(String[] args) {

        AWSCredentialsProvider credentials = DefaultAWSCredentialsProviderChain.getInstance();

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard().withRegion(Regions.US_EAST_1).build();

        // 1gram Counter:
        HadoopJarStepConfig jar1 = new HadoopJarStepConfig()
                .withJar(ONE_GRAM_URL_JAR)
                .withArgs(ONE_GRAM_CORPUS, ONE_GRAM_OUT_URL);

        StepConfig step1 = new StepConfig()
                .withName(ONE_GRAM_NAME)
                .withHadoopJarStep(jar1)
                .withActionOnFailure(TERM_JFLOW);

        // 2gram Counter:
        HadoopJarStepConfig jar2 = new HadoopJarStepConfig()
                .withJar(TWO_GRAM_URL_JAR)
                .withArgs(TWO_GRAM_CORPUS, TWO_GRAM_OUT_URL);

        StepConfig step2 = new StepConfig()
                .withName(TWO_GRAM_NAME)
                .withHadoopJarStep(jar2)
                .withActionOnFailure(TERM_JFLOW);

        // 3gram Counter:
        HadoopJarStepConfig jar3 = new HadoopJarStepConfig()
                .withJar(THREE_GRAM_URL_JAR)
                .withArgs(THREE_GRAM_CORPUS, THREE_GRAM_OUT_URL);

        StepConfig step3 = new StepConfig()
                .withName(THREE_GRAM_NAME)
                .withHadoopJarStep(jar3)
                .withActionOnFailure(TERM_JFLOW);

        // One & Three Join:
        HadoopJarStepConfig jar4 = new HadoopJarStepConfig()
                .withJar(ONE_THREE_JOIN_JAR)
                .withArgs(ONE_THREE_JOIN_INPUTS[0], ONE_THREE_JOIN_INPUTS[1], ONE_THREE_JOIN_OUTPUT);

        StepConfig step4 = new StepConfig()
                .withName(ONE_THREE_JOIN_NAME)
                .withHadoopJarStep(jar4)
                .withActionOnFailure(TERM_JFLOW);

        // Two & Three Join:
        HadoopJarStepConfig jar5 = new HadoopJarStepConfig()
                .withJar(TWO_THREE_JOIN_JAR)
                .withArgs(TWO_THREE_JOIN_INPUTS[0], TWO_THREE_JOIN_INPUTS[1], TWO_THREE_JOIN_OUTPUT);

        StepConfig step5 = new StepConfig()
                .withName(TWO_THREE_JOIN_NAME)
                .withHadoopJarStep(jar5)
                .withActionOnFailure(TERM_JFLOW);

        // Prob Calculation:
        HadoopJarStepConfig jar6 = new HadoopJarStepConfig()
                .withJar(CALCULATE_PROB_JAR)
                .withArgs(CALCULATE_PROB_INPUTS[0], CALCULATE_PROB_INPUTS[1], CALCULATE_PROB_INPUTS[2],CALCULATE_PROB_OUTPUT);

        StepConfig step6 = new StepConfig()
                .withName(CALCULATE_PROB_NAME)
                .withHadoopJarStep(jar6)
                .withActionOnFailure(TERM_JFLOW);

        //Sort Phase:
        HadoopJarStepConfig jar7 = new HadoopJarStepConfig()
                .withJar(SORT_JAR)
                .withArgs(SORT_INPUT, SORT_OUTPUT);

        StepConfig step7 = new StepConfig()
                .withName(SORT_NAME)
                .withHadoopJarStep(jar7)
                .withActionOnFailure(TERM_JFLOW);



        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(NUM_OF_INSTANCES)
                .withMasterInstanceType(INSTANCE_TYPE)
                .withSlaveInstanceType(INSTANCE_TYPE)
                .withHadoopVersion(HADOOP_VERSION)
                .withEc2KeyName(EC2_KEY_NAME)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType(REGION));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName(APP_NAME)
                .withInstances(instances)
                .withSteps(step1, step2, step3, step4, step5, step6)
                .withJobFlowRole(EMR_EC2_ROLE)
                .withServiceRole(EMR_ROLE)
                .withReleaseLabel(EMR_RELEASE_LABEL)
                .withLogUri(BUCKET_LOGS);


        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
}
