package sortPhase;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import utils.DatasetFormat;
import utils.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Key implements WritableComparable<Key> {

    private Text words;
    private Text compWords;
    private DoubleWritable prob;

    public Key(Text key) {
        String[] keyArr = key.toString().split(Utils.DELIMITER);
        String[] words = keyArr[0].split(DatasetFormat.NGRAM_DELIMITER);
        this.words = new Text(keyArr[0]);
        this.compWords = new Text(words[0] + DatasetFormat.NGRAM_DELIMITER + words[1]);
        this.prob = new DoubleWritable(Double.parseDouble(keyArr[1]));
    }

    @Override
    public int compareTo(Key key) {
        int result = this.compWords.compareTo(key.compWords);
        if (result == 0)
            result = (-1) *  this.prob.compareTo(key.prob);

        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        words.write(dataOutput);
        prob.write(dataOutput);
//        compWords.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        words.readFields(dataInput);
        prob.readFields(dataInput);
    }

    @Override
    public String toString() {
        return words.toString() + Utils.DELIMITER + prob.toString();
    }
}
