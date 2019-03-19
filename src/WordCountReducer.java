import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable value=new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        AtomicInteger sum= new AtomicInteger();
        for (IntWritable val:values){
            sum.set(sum.get() + val.get());
        }
        value.set(sum.get());
        context.write(key, value);

    }
}
