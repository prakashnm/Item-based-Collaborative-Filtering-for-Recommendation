package com.hadoop.collaborativefiltering;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ComputeSimilarityDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new ComputeSimilarityDriver(),args);
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job1 = new Job(getConf(),"Compute Similarity Score Job");
		job1.setJarByClass(ComputeSimilarityDriver.class);
		
		job1.setMapperClass(ComputeSimilarityStep1Mapper.class);
		job1.setReducerClass(ComputeSimilarityStep1Reducer.class);

		job1.setInputFormatClass(TextInputFormat.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"\\Step1"));

		job1.waitForCompletion(true);
		
		Job job2 = new Job(getConf(),"Compute Similarity Score Job");
		job2.setJarByClass(ComputeSimilarityDriver.class);
		
		job2.setMapperClass(ComputeSimilarityStep2Mapper.class);
		job2.setReducerClass(ComputeSimilarityStep2Reducer.class);

		job2.setInputFormatClass(TextInputFormat.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]+"\\Step1\\part-r-00*"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"\\Step2"));

		job2.waitForCompletion(true);



		return 0;
	}
}
