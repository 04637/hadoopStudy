package mapreduce.temperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * todo
 *
 * @author: 04637@163.com
 * @date: 2019/6/4
 */
public class RunJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.137.101:9820");
        // InputFormat separator
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        System.setProperty("HADOOP_USER_NAME", "root");

        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "weather");
        job.setJarByClass(RunJob.class);
        job.setMapperClass(TempMapper.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setReducerClass(TempReducer.class);
        job.setPartitionerClass(TempPartitioner.class);
        job.setSortComparatorClass(TempSort.class);
        job.setGroupingComparatorClass(TempGroup.class);
        job.setNumReduceTasks(3);
        job.setOutputKeyClass(TempKey.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/usr/input/data/temperature"));
        Path path = new Path("/usr/output/data/temperature");
        if(fs.exists(path)){
            fs.delete(path, true);
        }

        FileOutputFormat.setOutputPath(job, path);

        job.waitForCompletion(true);
    }
}
