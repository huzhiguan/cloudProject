package com.left.peter.atomic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.left.peter.data.Constants;

public class AtomicSolution extends Configured implements Tool
{
	@Override
	public int run(final String[] arg0) throws Exception
	{
		if(arg0.length < 3 || arg0.length > 8)
		{
			System.err.println("Usage: AtomicSolution <input files directory> <outputpath> <Reducer number> <area size> <areas number> <new atomic rate> <All random>");
			System.err.print(arg0);
			System.exit(-1); 
		}
		
		final Configuration conf = new Configuration();
		
		if (arg0.length > 3)
		{
		//	Constants.AREA_LEN.setValue(Integer.valueOf(arg0[3]));
			conf.set(Constants.AREA_LEN.toString(), arg0[3]);
		}
		
		if (arg0.length > 4)
		{
		//	Constants.AREA_NUM.setValue(Integer.valueOf(arg0[4]));
			conf.set(Constants.AREA_NUM.toString(), arg0[4]);
		}
		
		if (arg0.length > 5)
		{
		//	Constants.NEW_ATOMICS.setValue(Integer.valueOf(arg0[5]));
			conf.set(Constants.NEW_ATOMICS.toString(), arg0[5]);
		}
		
		if (arg0.length > 6)
		{
		//	Constants.ALL_AREAS.setValue(Integer.valueOf(arg0[6]));
			conf.set(Constants.ALL_AREAS.toString(), arg0[6]);
		}
		
		if (arg0.length > 7)
		{
			conf.set(Constants.NEW_RATE.toString(), arg0[7]);
		}
		
		Job job = new Job(conf);
		job.setJarByClass(AtomicSolution.class); 
		job.setJobName("AtomicSolution");
		job.setNumReduceTasks(Integer.valueOf(arg0[2]));
		// Add all files in the directory into input path.
		final FileSystem fs = FileSystem.get(job.getConfiguration());
		for (final FileStatus status : fs.listStatus(new Path(arg0[0])))
		{
			if (!status.getPath().getName().contains("_SUCCESS"))
			{
				FileInputFormat.addInputPath(job, status.getPath());
			}
        }
		fs.close();
		FileOutputFormat.setOutputPath(job,new Path(arg0[1]));
		job.setMapperClass(AtomicMapper.class); 
		job.setMapOutputKeyClass(IntWritable.class); 
		job.setMapOutputValueClass(AtomicWritable.class);
		job.setOutputKeyClass(NullWritable.class); 
		job.setOutputValueClass(Text.class);
		job.setReducerClass(AtomicReducer.class); 
		job.waitForCompletion(true);
		
//		Job job1 = new Job(conf);
//		job1.setJarByClass(AtomicSolution.class); 
//		job1.setJobName("AtomicSolution2");
//		job1.setNumReduceTasks(Integer.valueOf(arg0[2]));
//		// Add all files in the directory into input path.
//		for (final FileStatus status : fs.listStatus(new Path(arg0[0])))
//		{
//			if (!status.getPath().getName().contains("_SUCCESS"))
//			{
//				FileInputFormat.addInputPath(job1, status.getPath());
//			}
//        }
//		fs.close();
//		FileOutputFormat.setOutputPath(job1,new Path(arg0[1]+"1"));
//		job1.setMapperClass(AtomicMapper.class); 
//		job1.setMapOutputKeyClass(IntWritable.class); 
//		job1.setMapOutputValueClass(AtomicWritable.class);
//		job1.setOutputKeyClass(NullWritable.class); 
//		job1.setOutputValueClass(Text.class);
//		job1.setReducerClass(AtomicReducer.class); 
//		System.exit(job1.waitForCompletion(true) ? 0:1);  
//		boolean success = job1.waitForCompletion(true); 

		System.exit(job.waitForCompletion(true) ? 0:1);  
		boolean success = job.waitForCompletion(true); 
		return success ? 0 : 1; 
	} 
	
	
	public static void main(String[] args) throws Exception
	{ 
		AtomicSolution driver = new AtomicSolution(); 
		int exitCode = ToolRunner.run(driver, args); 
		System.exit(exitCode);
	}
}
