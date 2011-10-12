/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.recipes.distinct;

import com.whitelabellabs.hadoop.base.rollup.RollupWrapper;
import com.whitelabellabs.hadoop.recipes.aggregator.AggregatorWrapper;
import com.whitelabellabs.hadoop.recipes.topn.TopNAggregator;

public abstract class DistinctWrapper extends RollupWrapper 
{

	public DistinctWrapper()
	{
		super();
		output_text = false;
	}

	@Override
	public void postProcess() 
	{
		new DistinctAggregator().run(output_location, postprocessed_output_location);
	}



}
