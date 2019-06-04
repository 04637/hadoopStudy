package mapreduce.temperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 数据分区
 *
 * @author: 04637@163.com
 * @date: 2019/6/4
 */
public class TempPartitioner extends Partitioner<TempKey, Text> {
    @Override
    public int getPartition(TempKey tempKey, Text text, int numPartitions) {
        // 假如 1949, 1950, 1951 3年
        // 1949-1949=0; 第一个分区
        // 1950-1949=1; 第二个分区
        // 1951-1949=2; 第三个分区
        return (tempKey.getYear() - 1949) % numPartitions;
    }
}
