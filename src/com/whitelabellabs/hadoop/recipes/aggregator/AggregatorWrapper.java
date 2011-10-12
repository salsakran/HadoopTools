/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.recipes.aggregator;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.whitelabellabs.hadoop.base.rollup.RollupWrapper;


public class AggregatorWrapper extends RollupWrapper
{
	public AggregatorWrapper()
	{
		job_name = "AggregatorWrapper";
		// locations
		input_base_location  = "/var/data/";
		rollup_location = "/tmp/rollups/";
		output_location = "/tmp/output";
	}
	
	public static void main(String args[])
	{
		new AggregatorWrapper().inner_main(args);
	}
	
	@Override
	public void mapLogFileLine(String line,
			OutputCollector<Text, LongWritable> output, Reporter reporter)
			throws IOException 
	{
		String[] toks = line.split(",");
		output.collect(new Text(toks[0]),new LongWritable(1));
	}
	

	


}
