/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base.jobs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import com.whitelabellabs.hadoop.base.status.WrappedJobClient;


public class JobTemplate extends RunnableJob
{
	boolean OVERWRITE_TARGETS = true;
	
	protected void init(JobConf conf)
	{   
        
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		// default to text
		conf.setMapOutputKeyClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(IdentityMapper.class);
        
        
        conf.setReducerClass(IdentityReducer.class);
        
        
        
        }

	
	protected void add_inputfile_to_conf(String input_file, JobConf conf, FileSystem fs) throws IOException
	{
		String inputs[] = input_file.split(",");
    	
		   for(String i : inputs)
		   {
			System.err.println("Adding Input File: " + i);
			Path input_path = new  Path(i);
			if(i.startsWith("s3"))
			{
				FileSystem s3FS = FileSystem.get(input_path.toUri(), conf);
				if(s3FS.exists(input_path))
				{
					
					FileInputFormat.addInputPath(conf, input_path);
				}
				else
					System.err.println("Skipping NonExistant Input File: " + i);
				
			}
			else if(fs.exists(input_path)  )
				FileInputFormat.addInputPath(conf, input_path);
			else
				System.err.println("Skipping NonExistant Input File: " + i);
		   }
		   
	}
	
	public void run(List<String> inputs, String output)
	{
			long start_time = System.currentTimeMillis();
			
		   JobConf job = new JobConf(JobTemplate.class);

		   // BEFORE init() to allow per job # of reducers!
	        try
	        {
	        	 JobClient client = new JobClient(job);
	             ClusterStatus status = client.getClusterStatus();
	             int num_reducers = status.getTaskTrackers() * 2;
	             job.setNumReduceTasks(num_reducers);
	        }
	        catch (IOException ex)
	        {
	        	ex.printStackTrace();
	        	System.err.println("Can't set the proper number of reducers!");
	        	
	        }
	        

		   
		   init(job);

		   // Renames suck on s3. Don't try to be cute on s3 output files
		   boolean overwrite = OVERWRITE_TARGETS && !output.startsWith("s3");
		   String temp_output_location = "";
		   
	        
	        if(!overwrite)
	        	FileOutputFormat.setOutputPath(job, new Path(output));
	        else
	        {
	        	temp_output_location = getTempFileName(output,job);
	        	FileOutputFormat.setOutputPath(job, new Path(temp_output_location));
	        }
	        job.setJobName(this.getClass().getName());
	        
	        try 
	        {
	        	FileSystem fileSys = FileSystem.get(job);

	        	for(String additional_input : inputs)
	        		add_inputfile_to_conf(additional_input, job, fileSys);
	        	
	        	System.out.println("Running Job: "+ getClass().getName());
	            RunningJob this_job = WrappedJobClient.runJob(job);
	            long end_time = System.currentTimeMillis();
	            
	            System.out.println("Job: " + getClass().getName() + " took: " + (end_time - start_time)/1000 + "s");
	            Counters counters = this_job.getCounters();
	            
	            if(overwrite)
	            {
	                fileSys.delete(new Path(output), true);
	                fileSys.rename(new Path(temp_output_location), new Path(output));
	            }
			}
	        catch (FileNotFoundException e) 
	        {
				e.printStackTrace();
			} 
	        catch (IOException e) {
				e.printStackTrace();
			}
	}
	
	
	
	// Helper functions
	private int MAX_TEMP_FILE_TRIES = 3; 
	public String getTempFileName(String FileName, JobConf conf) {
	    String temp_file_name = FileName + "-temp-" + System.currentTimeMillis();

	    // Make sure we don't have two instances using the same temp file name
	    try
	    {
	      FileSystem fileSys = FileSystem.get(conf);
	      int num_tries = 0;
	      while (fileSys.exists(new Path(temp_file_name)) && num_tries < MAX_TEMP_FILE_TRIES) 
	      {
	        temp_file_name = FileName + "-temp-" + System.currentTimeMillis();
	        num_tries++;
	      }

	      if (fileSys.exists(new Path(temp_file_name))) 
	      {
	        // Things went pearshaped
	        throw new RuntimeException("Can't create temp file");
	      }

	    } 
	    catch (IOException e) 
	    {
	      e.printStackTrace();
	    }

	    return temp_file_name;
	  }
	
}
