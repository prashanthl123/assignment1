package org.MR_Assignment;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MR_MapperTwo extends Mapper<LongWritable, Text, Text, IntWritable> {
 
 public void map(LongWritable offset, Text value, Context con) throws IOException,
   InterruptedException {
  String line = value.toString();
  String[] words = line.split(",");
  String key = words[0]+words[1];
  con.write(new Text(key), new IntWritable(0));
 }
}