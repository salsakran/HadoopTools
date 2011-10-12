/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.recipes.distinct;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class TestDistinct extends DistinctWrapper
{
	public TestDistinct()
	{
		job_name = "TestDistinct";
		// locations
		input_base_location  = "/var/data/";
		rollup_location = "/tmp/distinct/rollups/";
		output_location = "/tmp/distinct/output";
		postprocessed_output_location= "/tmp/distinct/processed_output";
	}
	
	public static void main(String args[])
	{
		new TestDistinct().inner_main(args);
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
