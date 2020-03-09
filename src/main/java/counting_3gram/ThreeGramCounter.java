package counting_3gram;

import counting_1gram.OneGramCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.DatasetFormat;
import utils.Utils;

import java.io.IOException;

public class ThreeGramCounter {
    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split(DatasetFormat.DELIMITER);
            if (!Utils.preProcessing(record[DatasetFormat.N_GRAM].split(DatasetFormat.NGRAM_DELIMITER)))
                return;
            Text outKey = new Text(record[DatasetFormat.N_GRAM].trim()); // <w1,w2,w3>
            LongWritable outValue = new LongWritable(Long.parseLong(record[DatasetFormat.OCC])); // numOfOccurrences in a certain year
            context.write(outKey, outValue);

        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text text, LongWritable text2, int i) {
            return (text.hashCode() & Integer.MAX_VALUE) % i; // Non-negative
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            LongWritable outValue = new LongWritable(0);
            for (LongWritable value :
                    values) {
                sum += value.get();
            }
            outValue.set(sum);
            context.write(key, outValue);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "ThreeGramCounter");
        job.setJarByClass(ThreeGramCounter.class);
        // input format class :
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // output format class :
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Mapper & Reducer Classes :
        job.setMapperClass(ThreeGramCounter.Map.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ThreeGramCounter.Reduce.class);
        // Mapper Output Classes :
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // Output K&V Classes :
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //Combiner
        job.setCombinerClass(OneGramCounter.Reduce.class);
        //misc
        job.setNumReduceTasks(1);
        boolean success = job.waitForCompletion(true);
        System.out.println(success);
    }
}
