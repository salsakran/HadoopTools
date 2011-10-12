/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base.status;

import java.io.IOException;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

public class WrappedJobClient   
{
	
	
	public static RunningJob runJob(JobConf job) throws IOException 
	{
		try
		{
			RunningJob result = null;
			StatusUtils.getStatusHandler().jobStarted(job.getJobName());
			result = JobClient.runJob(job);
			StatusUtils.getStatusHandler().jobFinished(job.getJobName());
			return result;
		} 
		catch (IOException e) 
		{
			StatusUtils.getStatusHandler().jobFailed(job.getJobName());
			throw e;
		}
		
	}
}
