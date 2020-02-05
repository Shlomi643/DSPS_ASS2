package final_step;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Key, LongWritable> {

    private final int NGRAM = 0;
    private final int COUNT = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] vals = value.toString().split("\t");
        if (vals.length < 4)
            return;
        String[] ngram = vals[NGRAM].split(" ");
        if (ngram.length != 3)
            return;
        LongWritable count = new LongWritable(Long.parseLong(vals[COUNT]));
        LongWritable one = new LongWritable(1);
        String first = ngram[0], second = ngram[1], third = ngram[2];
        context.write(new Key(first, "*", "*"), one);           // For C0
        context.write(new Key("*", second, "*"), one);          // For C1
        context.write(new Key("*", "*", third), one);           // For N1
        context.write(new Key(first, second, "*"), one);        // For C2
        context.write(new Key("*", second, third), one);        // For N2
        context.write(new Key(first, second, third), count);    // For N3
    }
}
