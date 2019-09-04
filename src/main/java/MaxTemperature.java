import mapper.MaxTemperatureMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.MaxTemperatureReducer;

import java.io.IOException;

public class MaxTemperature{

    public static void main(String[] args) throws Exception{
        if(args.length != 2){
            System.err.println("Usage:MaxTemperatuerCombiner<input path>"+ "<output path>");
            System.exit(-1);
        }
        //获取hdfs系统配置
        Configuration conf = new Configuration();
        //获取作业
        Job job = Job.getInstance(conf);
        //设置程序入口类
        job.setJarByClass(MaxTemperature.class);
        //设置map类
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        //设置reduce类
        job.setReducerClass(MaxTemperatureReducer.class);
        //设置输出字段类型 key 和value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置要输出的文件的目录路径
        FileInputFormat.addInputPath(job, new Path("/data/MaxTemperature"));
        FileOutputFormat.setOutputPath(job, new Path("/data/MaxTemperature/output"));

        System.exit(job.waitForCompletion(true)? 0:1);
    }
}