/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base.jobs;

import java.util.ArrayList;
import java.util.List;

abstract public class JobWrapper 
{
	List<String> input_files;
	
	
	public JobWrapper()
	{
		input_files = new ArrayList<String>();
	}
	
	public void run(String input, String output) 
	{

		input_files.add(input);
		do_important_stuff(input_files,output);
		
	}

	abstract public void do_important_stuff(List<String> inputs,String output);
	
	public void addInputPath(String input_location) 
	{
		input_files.add(input_location);
		
	}

	
}
