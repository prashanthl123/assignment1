package org.MR_Assignment;
	
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MR_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

 public void reduce(Text key, Iterable<IntWritable> salaries,
   Context context) throws IOException, InterruptedException {
 System.out.println("MyReducer.reducer()"+key);
 System.out.println("hello from reducer");
	 
	 int total=0;
  while(salaries.iterator().hasNext()) {
   total += salaries.iterator().next().get();

  } 
  System.out.println(total);
  if(total>0){
  context.write(key, new IntWritable(total));
  }
 
}
}
