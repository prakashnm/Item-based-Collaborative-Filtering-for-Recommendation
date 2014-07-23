package com.hadoop.collaborativefiltering;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MovieFindMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	public void map(LongWritable inputKey,Text inputVal,Context context) throws IOException,InterruptedException
	{
		Configuration config= context.getConfiguration();
		String searchCustid=config.get("Search.Movieid");
		
		String line = inputVal.toString();
		String[] splits = line.trim().split(",");
			if (splits[1].equals(searchCustid))
				context.write(inputVal,NullWritable.get());

	}

}
