package sortPhase;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {

    protected KeyComparator() {
        super(Key.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Key k1 = ((Key) a);
        Key k2 = ((Key) b);
        return k1.compareTo(k2);
    }
}
