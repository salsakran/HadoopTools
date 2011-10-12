/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base.rollup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.TextInputFormat;


import com.whitelabellabs.hadoop.base.jobs.JobTemplate;

public class RollupMapper extends JobTemplate
{
	 
		// boilerplate
	
		Class<? extends Mapper> this_mapper;
		public  RollupMapper(Class<? extends Mapper> SpecificMapper)
		{
			this_mapper = SpecificMapper;	
		}
		
	
		protected void init(JobConf conf)
		{
			super.init(conf);
		    conf.setInputFormat(TextInputFormat.class);
		    conf.setSpeculativeExecution(false);
		    conf.setMapperClass(this_mapper);
		    conf.setCombinerClass(SumTextKeyedValues.reducer.class);
		    conf.setReducerClass(SumTextKeyedValues.reducer.class);
		    conf.setMapOutputKeyClass(Text.class);
		    conf.setMapOutputValueClass(LongWritable.class);
		    conf.setOutputKeyClass(Text.class);
		    conf.setOutputValueClass(LongWritable.class);
			
		}
}
