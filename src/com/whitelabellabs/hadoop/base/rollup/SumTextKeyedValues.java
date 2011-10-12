/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
/**
 * 
 */
package com.whitelabellabs.hadoop.base.rollup;

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

public class SumTextKeyedValues extends JobTemplate
{
	public static class reducer extends MapReduceBase implements Reducer<Text,LongWritable,Text,LongWritable>
	{
		LongWritable l = new LongWritable();
		@Override
	    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter)
	            throws IOException
	    {
			long total  = 0;
			while(values.hasNext())
			{
				total += values.next().get();
			}
			l.set(total);
			output.collect(key,l);
	    }
	}
	// boilerplate
	boolean OutputText;
	
	public SumTextKeyedValues()
	{
		this(true);
	}
	public SumTextKeyedValues(boolean OutputText)
	{
		super();
		this.OutputText = OutputText;
	}
	
	protected void init(JobConf conf)
	{
		super.init(conf);
		conf.setReducerClass(SumTextKeyedValues.reducer.class);
		conf.setOutputValueClass(LongWritable.class);
		conf.setMapOutputValueClass(LongWritable.class);
		if(OutputText)
			conf.setOutputFormat(TextOutputFormat.class);
		
	}
}
