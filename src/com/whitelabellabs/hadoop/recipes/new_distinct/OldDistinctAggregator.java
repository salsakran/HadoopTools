/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.recipes.new_distinct;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.whitelabellabs.hadoop.base.Histogram;
import com.whitelabellabs.hadoop.base.jobs.JobTemplate;

public class OldDistinctAggregator extends JobTemplate
{
	
	static class reducer extends MapReduceBase implements Reducer<Text,LongWritable,Text,LongWritable>
	{
		
		LongWritable new_value = new LongWritable();
		@Override
	    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter)
	            throws IOException
	    {
			long total = 0;
			while(values.hasNext())
			{
				total += values.next().get();
			}
			
			new_value.set(-total);
			output.collect(key, new_value);
			reporter.incrCounter("Distinct", "Total", total);
			reporter.incrCounter("Distinct - Spread", Histogram.getBinTagForLong(total), 1);
			
	    }
	}


	
	@Override
	public void run(List<String> inputs, String output) 
	{
		// Ignore the current date!
		super.run(inputs.subList(0, inputs.size()-1), output);
	}
	
	
	
	// boilerplate
	
	public OldDistinctAggregator()
	{
	}


	protected void init(JobConf conf)
	{
		super.init(conf);
		conf.setReducerClass(reducer.class);
		conf.setMapOutputValueClass(LongWritable.class);
		conf.setOutputValueClass(LongWritable.class);
	}
}
