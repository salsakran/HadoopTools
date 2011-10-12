/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.recipes.new_distinct;

import java.io.IOException;

import com.whitelabellabs.hadoop.base.jobs.RunnableJob;
import com.whitelabellabs.hadoop.base.rollup.MapAndAggregateWrapper;
import com.whitelabellabs.hadoop.base.rollup.RollupMapper;
import com.whitelabellabs.hadoop.base.rollup.RollupWrapper;
import com.whitelabellabs.hadoop.recipes.distinct.DistinctAggregator;

public abstract class NewDistinctWrapper extends RollupWrapper 
{

	@Override
	protected void createMapAndAggregateWrapper() throws IOException 
	{
		RunnableJob mapper =  new RollupMapper(this.getClass());
		RunnableJob aggregator =  new OldDistinctAggregator(); 
		wr = new MapAndAggregateWrapper(dates_of_interest, output_location, rollup_location, input_base_location, mapper,aggregator);

	}

	
	public NewDistinctWrapper()
	{
		super();
		output_text = false;
	}

	@Override
	public void postProcess() 
	{
		String tmp_location = "/tmp" + postprocessed_output_location;
		new GetNewDistinctsAggregator().run(output_location+ "," + 
											rollup_location + "/" + dates_of_interest.get(dates_of_interest.size()-1), 
											tmp_location);
		new DistinctAggregator().run(tmp_location, postprocessed_output_location);

	}



}
