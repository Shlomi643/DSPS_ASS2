package final_step;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class Key implements WritableComparable<Key> {

    private Text firstWord;
    private Text secondWord;
    private Text thirdWord;
    private String condition;

    public Key() {
        this.firstWord = new Text("*");
        this.secondWord = new Text("*");
        this.thirdWord = new Text("*");
        this.condition = "000";
    }

    public Key(String first, String second, String third) {
        this.firstWord = new Text(first);
        this.secondWord = new Text(second);
        this.thirdWord = new Text(third);
        this.condition = setCondition();
    }

    private String setCondition() {
        return List.of(firstWord, secondWord, thirdWord).stream()
                .map(x -> x.toString().equals("*") ? "0" : "1").collect(Collectors.joining(""));
    }

    public String getCondition() {
        return condition;
    }

    public int compareTo(Key o) {
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {
        firstWord.write(dataOutput);
        secondWord.write(dataOutput);
        thirdWord.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        firstWord.readFields(dataInput);
        secondWord.readFields(dataInput);
        thirdWord.readFields(dataInput);
    }

    private boolean isFirst() {
        return !firstWord.toString().equals("*");
    }

    private boolean isSecond() {
        return !secondWord.toString().equals("*");
    }

    private boolean isThird() {
        return !thirdWord.toString().equals("*");
    }

    public boolean isOnlyOne() {
        return isOnlyFirst() || isOnlySecond() || isOnlyThird();
    }

    public boolean isOnlyFirst() {
        return isFirst() && !isSecond() && !isThird();
    }

    public boolean isOnlySecond() {
        return !isFirst() && isSecond() && isThird();
    }

    public boolean isOnlyThird() {
        return !isFirst() && !isSecond() && isThird();
    }

    public boolean isFirstPair() {
        return isFirst() && isSecond() && !isThird();
    }

    public boolean isSecondPair() {
        return !isFirst() && isSecond() && isThird();
    }

    public boolean isAll() {
        return isFirst() && isSecond() && isThird();
    }

    public boolean isNone() {
        return !isFirst() && !isSecond() && !isThird();
    }
}
