package mapreduce.temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * todo
 *
 * @author: 04637@163.com
 * @date: 2019/6/4
 */
public class TempSort extends WritableComparator {

    public TempSort(){
        super(TempKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TempKey key1 = (TempKey)a;
        TempKey key2 = (TempKey)b;

        int r1 = Integer.compare(key1.getYear(), key2.getYear());
        if(r1 == 0){
            int r2 = Integer.compare(key1.getMonth(), key2.getMonth());
            if(r2 == 0){
                return -Double.compare(key1.getAir(), key2.getAir());
            }
            return r2;
        }
        return r1;
    }
}
