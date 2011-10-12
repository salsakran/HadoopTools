/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base.jobs;

import java.util.ArrayList;
import java.util.List;

public abstract class RunnableJob 
{
	
	public void run(String input, String output) 
	{
		List<String> input_container = new ArrayList<String>();
		input_container.add(input);
		run(input_container, output);
	}
	
	
	abstract public void run(List<String> inputs, String output) ;
}
