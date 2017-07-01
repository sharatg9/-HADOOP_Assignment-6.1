package task8;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = new Job (conf ,"task8");
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setJarByClass(Driver.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0])) ;
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}


}
