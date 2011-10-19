/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base.rollup;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.whitelabellabs.hadoop.base.DateUtils;
import com.whitelabellabs.hadoop.base.jobs.RunnableJob;


public abstract class RollupWrapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>
{
	protected String job_name = "RollupWrapper";
	// locations
	protected String date_seperator = "_";
	protected String input_base_location  = "/inputs/CHANGE_ME/";
	protected String rollup_location = "/rollups/CHANGE_ME/";
	protected String output_location = "/outputs/CHANGE_ME/";
	protected String postprocessed_output_location = "/processed_outputs/CHANGE_ME/";
	protected boolean output_text = true;
	protected boolean postprocess_output = true;
	
	protected List<String> dates_of_interest;
	protected MapAndAggregateWrapper wr;
	
	// deal with input and run 
	public void inner_main(String args[])
	{
		try 
		{
			CommandLine cl = RollupWrapperCLI.parse(args);
			DateUtils.date_format = new SimpleDateFormat("yyyy"+date_seperator+"MM"+date_seperator+"dd"); // do this more elegantly!

			String end_date = (String)cl.getOptionValue('e');
			if(cl.hasOption('s'))
			{
				String start_date = (String)cl.getOptionValue('s');
				dates_of_interest = DateUtils.getDates(start_date, end_date);
				
			}
			else if(cl.hasOption('m'))
			{
				int num_months = Integer.parseInt((String)cl.getOptionValue('m'));
				dates_of_interest = DateUtils.getPriorMonths(end_date, num_months);
				
			}

			else if(cl.hasOption('n'))
			{
				int num_days = Integer.parseInt((String)cl.getOptionValue('n'));
				dates_of_interest = DateUtils.getDatesFromEnd(end_date, num_days);
				
			}
			else
			{
				System.err.println("Error parsing date");
				RollupWrapperCLI.printHelp(job_name);
				System.exit(1);
			}
			
			
			if(cl.hasOption('o'))
			{
				postprocessed_output_location += (String)cl.getOptionValue('o') + "/";
			}
			// tag on the date for all the int files
			output_location += end_date;
			postprocessed_output_location += end_date;
			
			createMapAndAggregateWrapper();
			wr.run();
			postProcess();
			
		}
		catch (ParseException e)
		{
			System.err.println("Error parsing date");
			RollupWrapperCLI.printHelp(job_name);
		}
		catch (IOException e) 
		{
			System.err.println("IO Error");
			e.printStackTrace();
		} catch (org.apache.commons.cli.ParseException e) {
			System.err.println("Error parsing date");
			RollupWrapperCLI.printHelp(job_name);
		}
		
	}
	

	protected void createMapAndAggregateWrapper() throws IOException
	{
		RunnableJob mapper =  new RollupMapper(this.getClass());
		RunnableJob aggregator =  new SumTextKeyedValues(output_text); 
		wr = new MapAndAggregateWrapper(dates_of_interest, output_location, rollup_location, input_base_location, mapper,aggregator);

		
	} 
	
	  // This is where the job specific mapper is set up and used
	  @Override
	  public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException 
	  {
	    try 
	    {
	      mapLogFileLine(value.toString(), output, reporter);
	    } 
	    catch (Exception e) 
	    {
	      // Log Errors
	      reporter.incrCounter("Custom Counters : Map : Errors", "Count", 1);
	    } 
	    catch (java.lang.OutOfMemoryError e) 
	    {
	      throw new RuntimeException(value.toString());
	    }
	  }
	  

	  // Extend me.
	  public abstract void mapLogFileLine(String line, OutputCollector<Text, LongWritable> output, Reporter reporter)
	      throws IOException;

	  
	  // If you want to do anything more with the results, below is the place!
	  public void postProcess()
	  {}
	  
}
