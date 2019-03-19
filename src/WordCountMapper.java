import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text,Text,IntWritable>{
    public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
        String TexttoString= value.toString();
        String[] strArr= TexttoString.split(" ");
        for (String s:strArr){
            Text contextKey = new Text(s);
            IntWritable contextValue = new IntWritable(1);
            context.write(contextKey,contextValue);

        }
    }
}
