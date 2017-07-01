package task8;


import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;


public class Mapper1 extends Mapper <Object, Text, Text, Text>                       //Movie mapper 
	 {
	
	 public void map(Object key, Text value, Context context)throws IOException, InterruptedException 
	 {
		 String record = value.toString();
				
	 
		 context.write(new Text(record), new Text("1"));  
		 
		 }
	 }
	 


