package mapreduce.temperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 需找到每年每月的三个最高温度时刻并排序  Map任务
 *
 * @author: 04637@163.com
 * @date: 2019/6/4
 */
public class TempMapper extends Mapper<Text, Text, TempKey, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] strArr = key.toString().split("-");
        TempKey tempKey = new TempKey();
        tempKey.setYear(Integer.parseInt(strArr[0]));
        tempKey.setMonth(Integer.parseInt(strArr[1]));
        tempKey.setAir(Double.parseDouble(value.toString()));
        context.write(tempKey, new Text(key.toString()+"\t"+value));
    }
}
