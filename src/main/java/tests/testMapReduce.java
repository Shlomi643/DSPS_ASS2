package tests;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Utils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class testMapReduce {
    public static class MapTest extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (Utils.extractKey(value).toString().equals("אב אחד הם"))
                context.write(Utils.extractKey(value), Utils.extractValue(value));
        }
    }

    public static class ReduceTest extends Reducer <Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> tmps = new LinkedList<>();
            values.forEach(x -> tmps.add(x.toString()));
            context.write(key, new Text(String.join("", tmps)));
        }
    }
}
