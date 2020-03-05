package final_step;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Key, LongWritable, Key, Value> {


    @Override
    protected void reduce(Key key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long sum = 0;;;
        for (LongWritable x : values)
            sum += x.get();

        switch (key.getCondition()) {
            case "100":
            case "010":
            case "001":
//                context.write(key, new LongWritable(sum));
                break;


        }
    }
}
