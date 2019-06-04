package mapreduce.temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 数据分组
 *
 * @author: 04637@163.com
 * @date: 2019/6/4
 */
public class TempGroup extends WritableComparator {

    public TempGroup(){
        super(TempKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TempKey key1 = (TempKey)a;
        TempKey key2 = (TempKey)b;

        // 以年做对比, 如果在同一年中就返回所在月份比较结果, 不在同一年就返回年比较结果
        int r1 = Integer.compare(key1.getYear(), key2.getYear());
        // 如果年相等
        if(r1 == 0){
            return Integer.compare(key1.getMonth(), key2.getMonth());
        }
        return r1;
    }
}
