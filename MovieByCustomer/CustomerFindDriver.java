package com.hadoop.collaborativefiltering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomerFindDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new CustomerFindDriver(),args);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration config=new Configuration();
		config.set("Search.Custid", args[2]);
		
		Job job1 = new Job(config,"Customer Find Job");
		job1.setJarByClass(CustomerFindDriver.class);
		
		job1.setMapperClass(CustomerFindMapper.class);

		job1.setInputFormatClass(TextInputFormat.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"\\CustomerSearch\\"));

		job1.waitForCompletion(true);
		
		return 0;
	}
}

