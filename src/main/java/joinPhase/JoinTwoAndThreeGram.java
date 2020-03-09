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
import utils.DatasetFormat;
import utils.Utils;

import java.io.IOException;

public class JoinTwoAndThreeGram {
    public final static String twoGramIdentifier = "2gram";
    public final static String threeGramIdentifier = "3gram";

    public static class TwoGramMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(Utils.extractKey(value), new Text(twoGramIdentifier + Utils.DELIMITER + Utils.extractValue(value)));
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
            Text keyW12 = new Text(threeGram[0] + DatasetFormat.NGRAM_DELIMITER + threeGram[1]);
            Text keyW23 = new Text(threeGram[1] + DatasetFormat.NGRAM_DELIMITER + threeGram[2]);
            Text valueW12 = new Text(threeGramIdentifier + Utils.DELIMITER + extractedKey.toString() + Utils.DELIMITER + Utils.C2IDENTIFIER);
            Text valueW23 = new Text(threeGramIdentifier + Utils.DELIMITER + extractedKey.toString() + Utils.DELIMITER + Utils.N2IDENTIFIER);
            context.write(keyW12, valueW12);
            context.write(keyW23, valueW23);
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
         * @param key     w1 w2
         * @param values  <twoGramIdentifier + actual value> or <ThreeGramIdentifier + 3gram + C2IDENTIFIER/N2IDENTIFIER>
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String actualValue = getActualValue(values);
            for (Text val : values) {
                if (getInputIdentifier(val).equals(threeGramIdentifier))
                    context.write(new Text(extractThreeGramKey(val)), new Text(getOutputIdentifier(val) + Utils.DELIMITER + actualValue));
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

        private String extractTwoGramVal(Text value) {
            return value.toString().split(Utils.DELIMITER)[1];
        }

        private String getActualValue(Iterable<Text> values) {
            for (Text val : values) {
                if (getInputIdentifier(val).equals(twoGramIdentifier))
                    return extractTwoGramVal(val);
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
        Job job = new Job(conf, "ReducerSideJoin");
        job.setJarByClass(JoinTwoAndThreeGram.class);
        job.setReducerClass(JoinTwoAndThreeGram.ReducerSideJoin.class);
        job.setPartitionerClass(PartitionerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TwoGramMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinTwoAndThreeGram.ThreeGramMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

