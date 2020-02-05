package utils;

import com.amazonaws.services.ec2.model.InstanceType;

public class Utils {

    public static final String CORPUS = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
    public static final String REGION = "us-east-1";

    //    public static final String EMR_RELEASE_LABEL = "emr-5.16.0";
    public static final String HADOOP_VERSION = "2.8.4";
    public static final String EC2_KEY_NAME = "dsps-ass1";
    public static final String SUBNET_ID = "subnet-0df26357";
    public static final String INSTANCE_TYPE = InstanceType.M4Large.toString();

    public static final int NUM_OF_INSTANCES = 20;

    public static final String BUCKET_NAME = "dsps2-dan-shlomi";
    public static final String BUCKET_URL = "s3n://" + BUCKET_NAME + "/";
    public static final String BUCKET_LOGS = BUCKET_URL + "logs/";
    public static final String JAR_URL = BUCKET_URL + "jars/step1.jar";
    public static final String OUTPUT_URL = BUCKET_URL + "output/";


//    public static final String JARS_BUCKET_NAME = "dsps2-jars";

}
