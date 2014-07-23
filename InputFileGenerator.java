package com.hadoop.collaborativefiltering;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class InputFileGenerator {
	
	public static void main(String args[]) throws IOException
	{
		FileSystem fs=null;
		FSDataOutputStream fos=null;
		
		int MOVIES_TOT=10;
		int CUST_TOT=100;
		
		try
		{
			Configuration conf=new Configuration();
			conf.set("fs.defaultFS", "hdfs://localhost:54310");
			Path moviepath= new Path("/home/training/hadoop-temp/assignment/custMovieRatings");
			System.out.println(conf.get("fs.defaultFS"));
			fs=FileSystem.get(conf);
			
			if(fs.exists(moviepath))
			{
				fs.delete(moviepath,true);
			}
			
			fos=fs.create(moviepath);
			FsPermission perm=new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE);
			fs.setPermission(moviepath, perm);
			for(int i=1001;i<=1000+MOVIES_TOT;i++)
				for(int j=10001;j<=10000+CUST_TOT;j++)
				{
					StringBuffer sb=new StringBuffer();
					sb.append(j+","+i+","+((int)(Math.random()*5)+1)+"\n");
					fos.write(sb.toString().getBytes());
					System.out.println(i);
				}
			fos.sync();
		}
		finally
		{
			if(fos!=null)
				fos.close();
			if(fs!=null)
				fs.close();
		}
		System.out.println("Done");
	}

}
