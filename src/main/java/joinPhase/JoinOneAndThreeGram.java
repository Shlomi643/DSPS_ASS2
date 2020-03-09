package joinPhase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.DatasetFormat;
import utils.Utils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class JoinOneAndThreeGram {
    public final static String oneGramIdentifier = "1gram";
    public final static String threeGramIdentifier = "3gram";


    public static class OneGramMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            context.write(Utils.extractKey(value), new Text(oneGramIdentifier + Utils.DELIMITER + Utils.extractValue(value)));
        }

    }

    /**
     * Output Format: <w2/w3, identifier w1 w2 w3 c1/n1 (placeholder)>
     */
    public static class ThreeGramMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text extractedKey = Utils.extractKey(value);
            String[] threeGram = extractedKey.toString().split(DatasetFormat.NGRAM_DELIMITER);
            Text keyW2 = new Text(threeGram[1]);
            Text keyW3 = new Text(threeGram[2]);
            Text valueW2 = new Text(threeGramIdentifier + Utils.DELIMITER + extractedKey.toString() + Utils.DELIMITER + Utils.C1IDENTIFIER);
            Text valueW3 = new Text(threeGramIdentifier + Utils.DELIMITER + extractedKey.toString() + Utils.DELIMITER + Utils.N1IDENTIFIER);
            Text valueC0 = new Text(threeGramIdentifier + Utils.DELIMITER + extractedKey.toString() + Utils.DELIMITER + Utils.C0IDENTIFIER);
            context.write(keyW2, valueW2);
            context.write(keyW3, valueW3);
            context.write(Utils.C0KEY, valueC0);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text text, Text text2, int i) {
            return (text.hashCode() & Integer.MAX_VALUE) % i; // Non-negative
        }
    }

    public static class ReducerSideJoin extends Reducer<Text, Text, Text, Text> {
        /**
         * @param key     c0Key or w
         * @param values  has numerous formats:
         *                1. key = c0Key => value = <OneGramIdentifier + actual value> or <ThreeGramIdentifier + 3gram + C0IDENTIFIER>
         *                2. key = w =>     value = <OneGramIdentifier + actual value> or <ThreeGramIdentifier + 3gram + C1IDENTIFIER/N1IDENTIFIER>
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String actualValue = getActualValue(values);
            for (Text val :
                    values) {
                if (getInputIdentifier(val).equals(threeGramIdentifier))
                    consOutput(extractThreeGramKey(val), getOutputIdentifier(val), actualValue, context);
            }


        }

        private String getInputIdentifier(Text value) {
            return value.toString().split(Utils.DELIMITER)[0];
        }

        private String getOutputIdentifier(Text value) {
            String[] valComponents = value.toString().split(Utils.DELIMITER);
            return valComponents[valComponents.length - 1];
        }


        private String extractThreeGramKey(Text value) {
            return value.toString().split(Utils.DELIMITER)[1];
        }

        private String extractOneGramVal(Text value) {
            return value.toString().split(Utils.DELIMITER)[1];
        }

        private String getActualValue(Iterable<Text> values) {
            for (Text val : values) {
                if (getInputIdentifier(val).equals(oneGramIdentifier))
                    return extractOneGramVal(val);
            }
            return null;
        }

        /**
         * Output : <w1 w2 w3,identifier actualValue>
         *
         * @param key
         * @param identifier
         * @param actualValue
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        private void consOutput(String key, String identifier, String actualValue, Context context) throws IOException, InterruptedException {
            context.write(new Text(key), new Text(identifier + Utils.DELIMITER + actualValue));
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "JoinOneAndThreeGram");
        job.setJarByClass(JoinOneAndThreeGram.class);
        job.setPartitionerClass(PartitionerClass.class);
        // input format class :
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinOneAndThreeGram.OneGramMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinOneAndThreeGram.ThreeGramMapper.class);
        // output format class :
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job,outputPath);
        // Mapper & Reducer Classes :
        job.setReducerClass(ReducerSideJoin.class);
        // Mapper Output Classes :
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // Output K&V Classes :
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        boolean success = job.waitForCompletion(true);
        System.out.println(success);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}