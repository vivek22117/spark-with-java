package com.vivek.spark.sprakJobs;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.vivek.spark.utility.GzipUtility;
import com.vivek.spark.utility.JsonUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import static com.vivek.spark.utility.GzipUtility.compressData;
import static java.nio.file.Files.*;
import static java.util.stream.Collectors.toList;


public class EmrSparkExample {

    private static final Logger log = LoggerFactory.getLogger(EmrSparkExample.class);
    private static final String BUCKET_NAME = "cf-templates-show-ap-south-1";
    private static String keyName = "2018/08/data.text";

    private static JsonUtility jsonUtility;
    private static GzipUtility gzipUtility;

    private static AmazonS3 s3;
    private static AmazonElasticMapReduce emr;

    private String emrVersion = "emr-5.4.0";
    private String bucketName = "com.your.bucket";
    private boolean keepAlive = true;
    private String instanceType = "m3.xlarge";
    private int instanceCount = 3;
    private String uberJar = "./target/dl4j-spark-0.8-SNAPSHOT-bin.jar";
    private String className = "org.deeplearning4j.stats.TrainingStatsExample";

    public static void main(String[] args) {

        List<Employee> employees = Arrays.asList(new Employee("Vivek", "Yash"), new Employee("Harsha", "TIAA"),
                new Employee("Arti", "3M"), new Employee("RAJ", "SM"), new Employee("VIkash", "IIFL"),
                new Employee("Varun", "Army"), new Employee("RAJKumar", "Rlya"), new Employee("Ankit", "Yash"));

        try {
            init();
            uploadFileToS3(employees);
            uploadFileToS3FromFile();
            readeFromS3();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void init() throws Exception {
        jsonUtility = new JsonUtility();
        gzipUtility = new GzipUtility();
        AWSCredentialsProvider credentials = new ProfileCredentialsProvider("default");

        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentials)
                .withRegion(Regions.AP_SOUTH_1)
                .build();
       /* emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentials)
                .withRegion(Regions.AP_SOUTH_1)
                .build();*/
    }

    private static void uploadFileToS3FromFile() {
        byte[] bytes = readJsonFile("data/data_json.json");
        InputStream inputStream = new ByteArrayInputStream(bytes);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        PutObjectRequest objectRequest = new PutObjectRequest(BUCKET_NAME, keyName, inputStream, metadata);

        PutObjectResult putObjectResult = s3.putObject(objectRequest);
        System.out.println(putObjectResult.getETag());
    }
    private static void readeFromS3() throws Exception {
        final AtomicBoolean isResultComplete = new AtomicBoolean(false);
        SelectObjectContentRequest objectContentRequest = createObjectContentRequest(BUCKET_NAME, keyName);

        try (OutputStream outputStream = new FileOutputStream("data/s3Select/")) {
            SelectObjectContentResult result = s3.selectObjectContent(objectContentRequest);
            InputStream recordsInputStream = result.getPayload().getRecordsInputStream(new SelectObjectContentEventVisitor() {
                @Override
                public void visit(SelectObjectContentEvent.StatsEvent event) {
                    System.out.println("Received bytes scanned.." + event.getDetails().getBytesProcessed());
                }

                @Override
                public void visit(SelectObjectContentEvent.EndEvent event) {
                    isResultComplete.set(true);
                    System.out.println("Received end event, result is complete");
                }
            });
            IOUtils.copy(recordsInputStream, outputStream);
        }
        if(!isResultComplete.get()){
            throw new Exception(" S3 select request is incomplete as end event is not received");
        }
    }

    private static SelectObjectContentRequest createObjectContentRequest(String bucketName, String keyName) {
        JSONInput jsonInput = new JSONInput().withType(JSONType.DOCUMENT);

        InputSerialization serialization = new InputSerialization()
                .withCompressionType(CompressionType.GZIP)
                .withJson(jsonInput);
        OutputSerialization outputSerialization = new OutputSerialization()
                .withJson(new JSONOutput().withRecordDelimiter("\n"));

        SelectObjectContentRequest request = new SelectObjectContentRequest()
                .withBucketName(bucketName)
                .withKey(keyName)
                .withExpressionType(ExpressionType.SQL)
                .withExpression("Select * from S3Object s limit 4")
                .withInputSerialization(serialization).withOutputSerialization(outputSerialization);
        return request;
    }

    private static void uploadFileToS3(List<Employee> employees) throws JsonProcessingException {
        byte[] bytes = jsonUtility.convertToString(employees).getBytes();
        byte[] compressedBytes = compressData(bytes);
        InputStream inputStream = new ByteArrayInputStream(compressedBytes);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(compressedBytes.length);
        PutObjectRequest objectRequest = new PutObjectRequest(BUCKET_NAME, keyName, inputStream, metadata);

        PutObjectResult putObjectResult = s3.putObject(objectRequest);
        System.out.println(putObjectResult.getETag());
    }

    public void createCluster(String[] args) {
        List<StepConfig> steps = new ArrayList<>();

        StepFactory stepFactory = new StepFactory(Regions.AP_SOUTH_1 + ".elasticmapreduce");
        StepConfig enableDebugging = new StepConfig()
                .withName("Enable Debugging")
                .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .withHadoopJarStep(stepFactory.newEnableDebuggingStep());
        steps.add(enableDebugging);

        log.info("execute spark step");

        HadoopJarStepConfig sparkStepConf = new HadoopJarStepConfig();
        sparkStepConf.withJar("command-runner.jar");
        sparkStepConf.withArgs("spark-submit", "--deploy-mode", "cluster", "--class",
                className, getS3UberJarUrl(), "-useSparkLocal", "false");

        ActionOnFailure action = ActionOnFailure.TERMINATE_JOB_FLOW;

        if (keepAlive) {
            action = ActionOnFailure.CONTINUE;
        }

        StepConfig sparkStep = new StepConfig()
                .withName("Spark Step")
                .withActionOnFailure(action)
                .withHadoopJarStep(sparkStepConf);
        steps.add(sparkStep);

        log.info("create spark cluster");

        Application sparkApp = new Application().withName("Spark");

        // service and job flow role will be created automatically when
        // launching cluster in aws console, better do that first or create
        // manually

        RunJobFlowRequest request = new RunJobFlowRequest().withName("Spark Cluster").withSteps(steps)
                .withServiceRole("EMR_DefaultRole").withJobFlowRole("EMR_EC2_DefaultRole")
                .withApplications(sparkApp).withReleaseLabel(emrVersion)
                .withLogUri(getS3BucketLogsUrl()).withInstances(new JobFlowInstancesConfig()
                        .withEc2KeyName("spark").withInstanceCount(instanceCount)
                        .withKeepJobFlowAliveWhenNoSteps(keepAlive).withMasterInstanceType(instanceType)
                        .withSlaveInstanceType(instanceType));

        RunJobFlowResult result = emr.runJobFlow(request);

        log.info(result.toString());
    }

    private static byte[] readJsonFile(String filePath) {

        try (Stream<String> lines = lines(Paths.get(filePath))) {
            List<String> collect = lines.collect(toList());
            String jsonString = new JsonUtility().convertToString(collect);
            return compressData(jsonString.getBytes());
        } catch (IOException e) {
            throw new RuntimeException("Unable to read and compress.." + e.getMessage());
        }
    }

    public String getS3UberJarUrl() {
        return getS3BucketUrl() + "/" + new File(uberJar).getName();
    }

    public String getS3BucketUrl() {
        return "s3://" + bucketName;
    }

    public String getS3BucketLogsUrl() {
        return getS3BucketUrl() + "/logs";
    }

    public AWSStaticCredentialsProvider getCredentialsProvider() {
        return new AWSStaticCredentialsProvider(new BasicAWSCredentials("", ""));
    }

}



class Employee{

    private int index;
    private String name;
    private String company;

    static int count;

    public Employee(String name, String company) {
        this.index = count++;
        this.name = name;
        this.company = company;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }
}
