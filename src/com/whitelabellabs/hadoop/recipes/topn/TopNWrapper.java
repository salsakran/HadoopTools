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

import com.whitelabellabs.hadoop.base.rollup.RollupWrapper;

abstract public class TopNWrapper extends RollupWrapper 
{

	public TopNWrapper()
	{
		super();
		output_text = false;
	}

	@Override
	public void postProcess() 
	{
		new TopNAggregator().run(output_location, postprocessed_output_location);
	}



}
