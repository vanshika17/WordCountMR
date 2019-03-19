import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job=new Job(configuration);
        //to set the main class for jar
        job.setJarByClass(WordCountDriver.class);
        //to set the number of reducer task
        job.setNumReduceTasks(3);
        //to set the mapper class
        job.setMapperClass(WordCountMapper.class);
        //to set the reducer class
        job.setReducerClass(WordCountReducer.class);
        //to set the output key type
        job.setOutputKeyClass(Text.class);
        //to set the output value type
        job.setOutputValueClass(IntWritable.class);
        //take the first argument as input file
        FileInputFormat.addInputPath(job,new Path(args[1]));
        //take output file path
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        //if output file already exists then delete existing
        FileSystem fileSystem= FileSystem.get(configuration);
        fileSystem.delete(new Path(args[2]));
        //wait for completion
        System.exit( job.waitForCompletion(true)?0:1);
    }
}
