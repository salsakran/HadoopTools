/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
/*
 *  Starting point for a simple job
 *  
 * */


package com.whitelabellabs.hadoop.base;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.whitelabellabs.hadoop.base.jobs.JobTemplate;

public class SimpleJob extends JobTemplate
	{
		
		// the "stuff"
		static class mapper extends MapReduceBase implements Mapper<Text,Text,Text,Text>
		{
		    @Override
		    public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		            throws IOException
		    {

		    }
		}
		
		static class reducer extends MapReduceBase implements Reducer<Text,Text,Text,Text>
		{
			@Override
		    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
		            throws IOException
		    {
		    }
		}

		
		
		// boilerplate
		
		protected void init(JobConf conf)
		{
			conf.setMapperClass(mapper.class);
			conf.setReducerClass(reducer.class);
			
		}

	}
