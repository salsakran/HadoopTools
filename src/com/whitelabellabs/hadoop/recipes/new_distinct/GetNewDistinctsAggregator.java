/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.recipes.new_distinct;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.whitelabellabs.hadoop.base.jobs.JobTemplate;

public class GetNewDistinctsAggregator extends JobTemplate
{
	
	static class reducer extends MapReduceBase implements Reducer<Text,LongWritable,Text,LongWritable>
	{
		
		LongWritable new_value = new LongWritable();
		@Override
	    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter)
	            throws IOException
	    {
			long cur_value = values.next().get();

			
			if(values.hasNext())
			{
				reporter.incrCounter("Values", "Old and New Entry Found", 1);
				return; 
			}
			if (cur_value < 0)
			{
				reporter.incrCounter("Values", "Only Old Entry Found", 1);
				return; 
			}
			
			reporter.incrCounter("Values", "New", 1);
			new_value.set(cur_value);
			output.collect(key, new_value);
			
	    }
	}


	// boilerplate


	protected void init(JobConf conf)
	{
		super.init(conf);
		conf.setReducerClass(reducer.class);
		conf.setMapOutputValueClass(LongWritable.class);
		conf.setOutputValueClass(LongWritable.class);
	}
}
