package sortPhase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.Utils;

import java.io.IOException;

public class Sort {
    public static class SortMapper extends Mapper<LongWritable, Text, Key, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Key(Utils.extractKey(value)), Utils.extractValue(value));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text text, Text text2, int i) {
            return (text.hashCode() & Integer.MAX_VALUE) % i; // Non-negative
        }
    }

    public static class SortReducer extends Reducer<Key, Text, Text, Text> {
        @Override
        protected void reduce(Key key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString()), new Text());
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "SortPhase");
        job.setJarByClass(Sort.class);
        job.setMapperClass(SortMapper.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setReducerClass(SortReducer.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setGroupingComparatorClass(ResultKeyComparatorForGrouping.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
