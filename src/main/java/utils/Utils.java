package utils;

import com.amazonaws.services.ec2.model.InstanceType;
import org.apache.hadoop.io.Text;

public class Utils {

    public static final String APP_NAME = "DSPS_ASS2_DS";

    public static final String ONE_GRAM_CORPUS = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
    public static final String TWO_GRAM_CORPUS = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
    public static final String THREE_GRAM_CORPUS = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";

    public static final String REGION = "us-east-1a";

    public static final String EMR_RELEASE_LABEL = "emr-5.16.0";
    public static final String HADOOP_VERSION = "2.8.4";
    public static final String EC2_KEY_NAME = "dsps-ass2";
    public static final String INSTANCE_TYPE = InstanceType.M4Large.toString();

    public static final String EMR_EC2_ROLE = "EMR_EC2_Role";
    public static final String EMR_ROLE = "EMR_Role";

    public static final String BUCKET_NAME = "dsps2-ds";
    public static final String BUCKET_URL = "s3n://" + BUCKET_NAME + "/";
    public static final String BUCKET_LOGS = BUCKET_URL + "logs/";
    public static final String OUTPUT_URL = BUCKET_URL + "output/";

    public static final int NUM_OF_INSTANCES = 10; // TODO: 08/03/2020 CHANGE

    public static final String TERM_JFLOW = "TERMINATE_JOB_FLOW";

    public static final String ONE_GRAM_URL_JAR = BUCKET_URL + "jars/OneGramCounter.jar";
    public static final String TWO_GRAM_URL_JAR = BUCKET_URL + "jars/TwoGramCounter.jar";
    public static final String THREE_GRAM_URL_JAR = BUCKET_URL + "jars/ThreeGramCounter.jar";

    public static final String ONE_GRAM_OUT_URL = OUTPUT_URL + "FirstStepOut";
    public static final String TWO_GRAM_OUT_URL = OUTPUT_URL + "SecondStepOut";
    public static final String THREE_GRAM_OUT_URL = OUTPUT_URL + "ThirdStepOut";

    public static final String ONE_GRAM_NAME = "OneGramCounter";
    public static final String TWO_GRAM_NAME = "TwoGramCounter";
    public static final String THREE_GRAM_NAME = "ThreeGramCounter";

    public static final String ONE_THREE_JOIN_NAME = "JoinOneAndThreeGram";
    public static final String[] ONE_THREE_JOIN_INPUTS = {ONE_GRAM_OUT_URL, THREE_GRAM_OUT_URL};
    public static final String ONE_THREE_JOIN_OUTPUT = OUTPUT_URL + "FourthStepOut";
    public static final String ONE_THREE_JOIN_JAR = BUCKET_URL + "jars/JoinOneAndThreeGram.jar";

    public static final String TWO_THREE_JOIN_NAME = "JoinTwoAndThreeGram";
    public static final String[] TWO_THREE_JOIN_INPUTS = {TWO_GRAM_OUT_URL, THREE_GRAM_OUT_URL};
    public static final String TWO_THREE_JOIN_OUTPUT = OUTPUT_URL + "FifthStepOut";
    public static final String TWO_THREE_JOIN_JAR = BUCKET_URL + "jars/JoinTwoAndThreeGram.jar";

    public static final String CALCULATE_PROB_NAME = "CalculateProbFormula";
    public static final String[] CALCULATE_PROB_INPUTS = {TWO_THREE_JOIN_OUTPUT, ONE_THREE_JOIN_OUTPUT, THREE_GRAM_OUT_URL};
    public static final String CALCULATE_PROB_OUTPUT = OUTPUT_URL + "sixthStepOut";
    public static final String CALCULATE_PROB_JAR = BUCKET_URL + "jars/CalculateProbFormula.jar"; //

    public static final String SORT_NAME = "Sort";
    public static final String SORT_INPUT = CALCULATE_PROB_OUTPUT;
    public static final String SORT_OUTPUT = OUTPUT_URL + "seventhStepOut";
    public static final String SORT_JAR = BUCKET_URL + "jars/Sort.jar"; //


    public final static Text C0KEY = new Text("@6xrY~");
    public final static String C0IDENTIFIER = "c0";
    public final static String C1IDENTIFIER = "c1";
    public final static String N1IDENTIFIER = "n1";
    public final static String C2IDENTIFIER = "c2";
    public final static String N2IDENTIFIER = "n2";
    public final static String N3IDENTIFIER = "n3";
    public final static String DELIMITER = "\t";


    public static boolean preProcessing(String [] words){
        for (String word : words){
            if (!word.trim().matches("[א-ת]*") || word.length() <= 1)
                return false;
        }
        return true;
    }

    public static Text extractKey(Text value){
        return new Text(value.toString().split(DELIMITER)[0]);
    }

    public static Text extractValue(Text value){
        String valS = value.toString();
        return new Text(valS.substring(valS.indexOf(DELIMITER) + 1));
    }

}
