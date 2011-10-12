/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base.rollup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.S3FileSystem;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.mapred.JobConf;

import com.whitelabellabs.hadoop.base.jobs.JobWrapper;
import com.whitelabellabs.hadoop.base.jobs.RunnableJob;



/***
 * 
 * This is a wrapper around the idea of having daily rollups of input data. Each day stores some 
 * specific projection of the input data. For the final product, one or more of these day's results are
 * aggregated (or otherwise processed) all together to produce a result.
 * 
 * Input files should be relative to the input location
 * 
 * @author as
 *
 */
public class MapAndAggregateWrapper
{
	// actual work happens in these 
	RunnableJob mapper;
	RunnableJob aggregator;
	
	// locations
	String input_base_location;
	String rollup_location;
	String output_location;
	
	boolean persist_rollups = true;
	boolean OverWrite = false;
	
	// What to aggregate
	List<String> input_files;
	JobConf conf;
	FileSystem input_fs;
	FileSystem rollup_fs;


	  public MapAndAggregateWrapper(List<String> input_items, String OutputLocation, String Rollups, String BaseLocations,RunnableJob Mapper, RunnableJob Aggregator, boolean overwrite) throws IOException 
	  {
		  input_files = input_items;
	    mapper = Mapper;
	    aggregator = Aggregator;
	    output_location = OutputLocation;
	    input_base_location = BaseLocations;
	    rollup_location = Rollups;
	    OverWrite = overwrite;


	    // If we can't get a file system, that's a problem. this throws an exception
	    rollup_fs = input_fs = FileSystem.get(new JobConf(MapAndAggregateWrapper.class));
	    if(rollup_location.startsWith("s3n"))
	    	rollup_fs = new NativeS3FileSystem();
	    else if(rollup_location.startsWith("s3n"))
	    	rollup_fs = new S3FileSystem();
	    assert(rollup_fs != null);
	  }

	  public MapAndAggregateWrapper(List<String> input_items, String OutputLocation, String Rollups, String BaseLocations,RunnableJob Mapper, RunnableJob Aggregator) throws IOException 
	  {
	    this(input_items, OutputLocation, Rollups, BaseLocations, Mapper, Aggregator, false);
	  }

	

	void map_single(String Item)
	{
	    String input_location = input_base_location + Item;
	    String result_location = rollup_location + Item;

		log("Mapping Item: " + Item + "\tInput: " + input_location + "\tOutput: " + result_location);
	    mapper.run(input_location, result_location);
	}
	
	void map_all() throws IOException
	{
		log("Mapping Input Files: ");
		for (String input_file : input_files) 
		{
		      String result_location = rollup_location + input_file;
		      System.out.println(result_location);
		      if (OverWrite || !safe_exists(new Path(result_location))) 
		      {
		        log(input_file + " Doesn't exist... regenerating");

		        map_single(input_file);
		      }
		      else 
		      {
		        log(result_location + " exists");
		      }
    	}
		
	}
	
	// Wrapping this due to https://issues.apache.org/jira/browse/HDFS-538
	boolean safe_exists(Path p)
	{
		try{
		if(rollup_fs.exists(p))
			return true;
		else
			return false;
		}
		catch(Exception e)
		{
			return false;
		}
	}
	
	void aggregate()
	{
		log("Aggregating");
		ArrayList<String> full_input_paths = new ArrayList<String>(input_files.size());
		
	    for(String item : input_files) 
	    {
	      String result_location = rollup_location + item;
	      log("adding: " + result_location);
	      full_input_paths.add(result_location);
	    }
	    aggregator.run(full_input_paths, output_location);

		
	}
	
	void run()
	{
		try 
		{
	      map_all();
	      aggregate();
	    }
		catch (IOException e) 
		{
	      log(e);
	    }
	}
	
	// Info on shit 
	void log(Throwable t)
	{
		t.printStackTrace();
		
	}
	void log(String line)
	{
		System.out.println(line);
	}
	

}
