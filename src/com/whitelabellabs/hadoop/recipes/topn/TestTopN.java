/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.recipes.topn;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class TestTopN  extends TopNWrapper
{
	public TestTopN()
	{
		job_name = "TestTopN";
		// locations
		input_base_location  = "/var/data/";
		rollup_location = "/tmp/topn_rollups/";
		output_location = "/tmp/topn_output";
		postprocessed_output_location= "/tmp/processed_topn_output";
	}
	
	public static void main(String args[])
	{
		new TestTopN().inner_main(args);
	}
	
	@Override
	public void mapLogFileLine(String line,
			OutputCollector<Text, LongWritable> output, Reporter reporter)
			throws IOException 
	{
		String[] toks = line.split(",");
		output.collect(new Text(toks[0] + ";"+ toks[1]),new LongWritable(1));
	}
	
}
