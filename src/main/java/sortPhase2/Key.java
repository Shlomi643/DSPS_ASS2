package sortPhase2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import utils.DatasetFormat;
import utils.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Key implements WritableComparable<Key> {

    private Text words;
    private Text compWords;
    private DoubleWritable prob;

    public Key(){
        this.words = new Text();
        this.compWords = new Text();
        this.prob = new DoubleWritable();
    }

    public Key(Text key) {
        String[] keyArr = key.toString().split(Utils.DELIMITER);
        String[] words = keyArr[0].split(DatasetFormat.NGRAM_DELIMITER);
        this.words = new Text(keyArr[0]);
        this.compWords = new Text(words[0] + DatasetFormat.NGRAM_DELIMITER + words[1]);
        this.prob = new DoubleWritable(Double.parseDouble(keyArr[1]));
    }

    public Text getWords() {
        return words;
    }

    public void setWords(Text words) {
        this.words = words;
    }

    public Text getCompWords() {
        return compWords;
    }

    public void setCompWords(Text compWords) {
        this.compWords = compWords;
    }

    public DoubleWritable getProb() {
        return prob;
    }

    public void setProb(DoubleWritable prob) {
        this.prob = prob;
    }

    @Override
    public int compareTo(Key key) {
        int result = this.getCompWords().compareTo(key.getCompWords());
        if (result == 0)
            result = (-1) *  this.getProb().compareTo(key.getProb());

        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        words.write(dataOutput);
        compWords.write(dataOutput);
        prob.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        words.readFields(dataInput);
        compWords.readFields(dataInput);
        prob.readFields(dataInput);
    }

    @Override
    public String toString() {
        return words.toString() + Utils.DELIMITER + prob.toString();
    }

//    public static void main(String[] args) {
//        Key key = new Key(new Text("את הדבר לשם\t0.001577559684064471\t"));
//        Key key2 = new Key(new Text("לא נודע אל\t0.011784379395437685\t"));
//        Key key3 = new Key(new Text("על זה כאילו\t0.0012808365766436287\t"));
//        Key key4 = new Key(new Text("את הדבר למען\t0.001402636251873713\t"));
//        Key key5 = new Key(new Text("אותה כל אימת\t0.008671181584867679\t"));
//        Key[] keys = {key, key2, key3, key4, key5};
//        Arrays.sort(keys);
//        System.out.println(key.compareTo(key2));
//        System.out.println(key.compareTo(key3));
//        System.out.println(key.compareTo(key4));
//        System.out.println(key.compareTo(key5));
//
//        for (Key key1 : keys) {
//            System.out.println(key1);
//        }
//
//    }
}
