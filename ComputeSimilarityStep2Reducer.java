package com.hadoop.collaborativefiltering;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ComputeSimilarityStep2Reducer extends Reducer<Text,Text,Text, Text>{

	@Override
	public void reduce(Text key,Iterable<Text> listOfValues,Context context) throws IOException,InterruptedException
	{
	
		float Score=0;
		int counter=0;
		for(Text val:listOfValues)
		{
			counter++;
			//taking average as score -- need to implement logic here
			String ratingPair=val.toString().replace("(", "").replace(")", "");
			String ratings[]=ratingPair.split(",");
				Score=(Float.parseFloat(ratings[0])+Float.parseFloat(ratings[1]))/2;
		}
		context.write(key,new Text(new Float(Score/counter).toString()));
	}

}
