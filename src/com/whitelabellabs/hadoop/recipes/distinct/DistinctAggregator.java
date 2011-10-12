/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.recipes.distinct;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.whitelabellabs.hadoop.base.Histogram;
import com.whitelabellabs.hadoop.base.jobs.JobTemplate;

public class DistinctAggregator extends JobTemplate
{
	
	// the "stuff"
	static class mapper extends MapReduceBase implements Mapper<Text,LongWritable,Text,Text>
	{
		Text new_key = new Text();
		Text new_value = new Text();
	    @Override
	    public void map(Text key, LongWritable value, OutputCollector<Text, Text> output, Reporter reporter)
	            throws IOException
	    {
	    	String toks[] = key.toString().split(";");
	    	new_key.set(toks[0]);
	    	new_value.set(toks[1]);
	    	output.collect(new_key, new_value);
	    }
	}

	
	static class reducer extends MapReduceBase implements Reducer<Text,Text,Text,LongWritable>
	{
		
		LongWritable new_value = new LongWritable();
		@Override
	    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, LongWritable> output, Reporter reporter)
	            throws IOException
	    {
			long num_values = 0;
			while(values.hasNext())
			{
				num_values++;
				values.next();
			}
			
			new_value.set(num_values);
			output.collect(key, new_value);
			reporter.incrCounter("Distinct", "Total", num_values);
			reporter.incrCounter("Distinct - Spread", Histogram.getBinTagForLong(num_values), 1);
			
	    }
	}

	
	
	// boilerplate
	
	public DistinctAggregator()
	{
	}

	protected void init(JobConf conf)
	{
		super.init(conf);
		conf.setMapperClass(mapper.class);
		conf.setReducerClass(reducer.class);
		conf.setOutputFormat(TextOutputFormat.class);
	}
}
