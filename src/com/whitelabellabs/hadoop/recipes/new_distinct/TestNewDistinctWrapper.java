/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.recipes.new_distinct;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.whitelabellabs.hadoop.recipes.distinct.DistinctWrapper;
import com.whitelabellabs.hadoop.recipes.distinct.TestDistinct;

public class TestNewDistinctWrapper extends NewDistinctWrapper
{
	public TestNewDistinctWrapper()
	{
		job_name = "TestNewDistinct";
		// locations
		input_base_location  = "/var/data/";
		rollup_location = "/tmp/new_distinct/rollups/";
		output_location = "/tmp/new_distinct/output";
		postprocessed_output_location= "/tmp/new_distinct/processed_output";
	}
	
	public static void main(String args[])
	{
		new TestNewDistinctWrapper().inner_main(args);
	}
	
	@Override
	public void mapLogFileLine(String line,
			OutputCollector<Text, LongWritable> output, Reporter reporter)
			throws IOException 
	{
		String[] toks = line.split(",");
		output.collect(new Text(toks[1] + ";"+ toks[2]),new LongWritable(1));
	}
	
}
