/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base.rollup;
import org.apache.commons.cli.*;

public class RollupWrapperCLI 
{
	
	static Options options = new Options();
	static Parser parser = new BasicParser();
	static
	{
		options.addOption("e", "end_date", true, "last day to include in this rollup (required)");
		options.addOption("h", "help", false, "Print this usage information");
		options.addOption("m", "num_months", true, "the number of months before the end date to process");
		options.addOption("n", "num_days", true, "the number of days before the end date to process");
		options.addOption("s", "start_date", true, "first day to include in this rollup");
		options.addOption("o", "output_prefix", true, "output prefix");
	}
	
		
	// Dumps to Std Out takes in the name of the job
	static void printHelp(String name)
	{
		HelpFormatter hf = new HelpFormatter();
		hf.printHelp("wrapper", options);
	}

	public static CommandLine parse(String[] args) throws ParseException  
	{
		
		

		return parser.parse(options,args);
	
	}
	
}
