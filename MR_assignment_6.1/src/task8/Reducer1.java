package task8;


import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reducer1 extends Reducer<Text, Text, Text, IntWritable>{

	 HashMap<String, Integer> map = new HashMap<String, Integer>();
	
	 public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
	 {
		 
		String[] token = key.toString().split("\t");
		 
		String key1 = token[0];
		int key2 = Integer.parseInt(token[1]);
		 map.put(key1,key2);
		 
	   }
	 
	 
	 @Override
     protected void cleanup(Context context) throws IOException, InterruptedException {
		 
		 Object[] a = map.entrySet().toArray();
			Arrays.sort( a , new Comparator() {
			    public int compare(Object o1, Object o2) {
			        return ((Map.Entry<String, Integer>) o2).getValue()
			                   .compareTo(((Map.Entry<String, Integer>) o1).getValue());
			    }
			});
			
			int count = 0 ;
			for (Object e : a) {
				String movie_name = ((Map.Entry<String, Integer>) e).getKey() ;
				int sum =  ((Map.Entry<String, Integer>) e).getValue(); 		
				
				if(count == 10 ){ break ; }
				
				context.write(new Text(movie_name), new IntWritable(sum));
				count++;
				  
			}
		 
		 
	 }
	 
}
