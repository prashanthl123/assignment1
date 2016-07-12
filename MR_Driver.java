package org.MR_Assignment;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MR_Driver {
 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
  
  Path input_dir1=new Path("hdfs://localhost:9000/user/prloka/employee.txt");
  Path input_dir2=new Path("hdfs://localhost:9000/user/prloka/employee_reference.txt");
  
     Path output_dir=new Path("hdfs://localhost:9000/output_data/");
  
  Configuration conf = new Configuration();
  

  Job job = new Job(conf, "MyWordCountJob");
  
  
     job.setJarByClass(MR_Driver.class);
  
     job.setMapperClass(MR_Mapper.class);
     job.setMapperClass(MR_MapperTwo.class);
    
     job.setReducerClass(MR_Reducer.class);
    
     job.setMapOutputKeyClass(Text.class);
     job.setMapOutputValueClass(IntWritable.class);
     job.setNumReduceTasks(1);
     
     MultipleInputs.addInputPath(job,input_dir1, TextInputFormat.class, MR_Mapper.class);
    
     MultipleInputs.addInputPath(job,input_dir2, TextInputFormat.class, MR_MapperTwo.class);
    
     FileOutputFormat.setOutputPath(job,output_dir );
    output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,true);
  
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    
  
    
 }
}