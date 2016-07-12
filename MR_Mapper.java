package org.MR_Assignment;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MR_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
 
 
 
 public void map(LongWritable offset, Text value, Context context) throws IOException,
   InterruptedException {

  String line = value.toString();
  String[] words = line.split(",");
  if(words[4].equals("developer"))
  {
  String key = words[0]+words[1];
  int salary= Integer.parseInt(words[2]+words[3]);
 
 
  System.out.println(key+"::"+salary);
  context.write(new Text(key), new IntWritable(salary));
  }
 }
}
