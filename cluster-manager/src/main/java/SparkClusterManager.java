import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

import java.util.*;


public class SparkClusterManager
{
    private static final SparkClusterManager SPARK_CLUSTER_MANAGER = new SparkClusterManager();
    private final AWSCredentialsProvider credentialsProvider;
    private final List<String> runJobFlowResultList = new ArrayList<>();
    private final List<String> stepIdsList = new ArrayList<>();
    private AmazonElasticMapReduce EMR_CLIENT;

    public SparkClusterManager()
    {
        try
        {
            credentialsProvider = new ProfileCredentialsProvider("default");
        }
        catch (Exception e)
        {
            throw new AmazonClientException("Some problem with AWS Credentials ... Best of 'Luck' trying to fix it ;-(((((( !! ", e);
        }
    }

    public static SparkClusterManager getSparkClusterManager()
    {
        return SPARK_CLUSTER_MANAGER;
    }

    public void startCluster()
    {
        String s3BucketClusterLogs = "s3://aws-emr-cluster-logs/main-cluster-log-folder/";

        System.out.println("Credentials got loaded successfully. ");
        EMR_CLIENT = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentialsProvider.getCredentials()))
                .withRegion(Regions.US_EAST_2)
                .build();

        // this is for enabling debugging in AWS Mmg console
        StepFactory stepFactory = new StepFactory("us-east-2.elasticmapreduce");
        StepConfig enabledebugging = new StepConfig()
                .withName("enable debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(stepFactory.newEnableDebuggingStep());

        Application spark = new Application().withName("Spark");

        Map<String, String> properties = new HashMap<>();
        properties.put("fs.s3n.endpoint", "s3.amazonaws.com");

        Configuration coreSite = new Configuration()
                .withClassification("core-site")
                .withProperties(properties);

        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("Spark Cluster Test")
                .withReleaseLabel("emr-5.33.0")
                .withSteps(enabledebugging)
                .withApplications(spark)
                .withConfigurations(coreSite)
                .withLogUri(s3BucketClusterLogs)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withVisibleToAllUsers(true)
                .withInstances(new JobFlowInstancesConfig()
                .withEc2SubnetId("subnet-0fcd8b73194cc0c24")
                .withEc2KeyName("myKeyPair")
                .withInstanceCount(3)
                .withKeepJobFlowAliveWhenNoSteps(true)
                .withMasterInstanceType("m4.large")
                .withSlaveInstanceType("m4.large"));

        RunJobFlowResult result = EMR_CLIENT.runJobFlow(request);
        runJobFlowResultList.add(result.getJobFlowId());
        System.out.println("Result of create AWS EMR cluster is " + result + "\n");
    }

    public void runJob(String jarLocation)
    {
        String clusterId = runJobFlowResultList.get(0);
        System.out.println("Submitting job to AWS EMR cluster: " + clusterId);

        AddJobFlowStepsRequest req = new AddJobFlowStepsRequest();
        req.withJobFlowId(clusterId);

        List<StepConfig> stepConfigs = new ArrayList<>();

        HadoopJarStepConfig sparkStepConf = new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withArgs("spark-submit","--executor-memory","1g","--class","org.example.App",jarLocation,"10");

        StepConfig sparkStep = new StepConfig()
                .withName("Spark Step")
                .withActionOnFailure("CONTINUE")
                .withHadoopJarStep(sparkStepConf);

        stepConfigs.add(sparkStep);
        req.withSteps(stepConfigs);
        AddJobFlowStepsResult result = EMR_CLIENT.addJobFlowSteps(req);
        stepIdsList.addAll(result.getStepIds());
        System.out.println("Result of running job on cluster: " + result);
    }

    public void terminateCluster()
    {
        TerminateJobFlowsRequest request = new TerminateJobFlowsRequest()
                .withJobFlowIds(runJobFlowResultList)
                .withRequestCredentialsProvider(credentialsProvider);

        TerminateJobFlowsResult result = EMR_CLIENT.terminateJobFlows(request);
        System.out.println("Result of cluster terminate operation: " + result + "\n");
    }
}
