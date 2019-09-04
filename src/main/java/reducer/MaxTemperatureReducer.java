package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int Maxvalue = Integer.MIN_VALUE;
        for(IntWritable value:values){
            Maxvalue = Math.max(Maxvalue, value.get());
        }
        context.write(key, new IntWritable(Maxvalue));
    }
}