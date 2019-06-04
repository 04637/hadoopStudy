package mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * todo
 *
 * @author: 04637@163.com
 * @date: 2019/6/2
 */
public class MRRunJob {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.137.101:9820");
        System.setProperty("HADOOP_USER_NAME", "root");

        Job job = null;
        try {
            job = Job.getInstance(conf, "mywc");
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(job == null){
            System.err.println("job == null");
            System.exit(1);
        }

        job.setJarByClass(MRRunJob.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        try {
            FileInputFormat.addInputPath(job, new Path("/usr/input/data/wc"));
            FileOutputFormat.setOutputPath(job, new Path("/usr/output/data/wc"));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
