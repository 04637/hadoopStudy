package mapreduce.temperature;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer任务
 *
 * @author: 04637@163.com
 * @date: 2019/6/4
 */
public class TempReducer extends Reducer<TempKey, Text, NullWritable, Text> {

    @Override
    protected void reduce(TempKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(Text t: values){
            sum++;
            // 取出前三个
            if(sum > 3){
                break;
            }else{
                context.write(NullWritable.get(), t);
            }
        }
    }
}
