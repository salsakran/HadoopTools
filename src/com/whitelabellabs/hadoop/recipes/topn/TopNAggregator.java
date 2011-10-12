/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.recipes.topn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.whitelabellabs.hadoop.base.MiscUtils;
import com.whitelabellabs.hadoop.base.TextWithFreq;
import com.whitelabellabs.hadoop.base.jobs.JobTemplate;

public class TopNAggregator extends JobTemplate
{
	
	// the "stuff"
	static class mapper extends MapReduceBase implements Mapper<Text,LongWritable,TextWithFreq,TextWithFreq>
	{
		TextWithFreq t = new TextWithFreq();
		TextWithFreq new_value = new TextWithFreq();
	    @Override
	    public void map(Text key, LongWritable value, OutputCollector<TextWithFreq, TextWithFreq> output, Reporter reporter)
	            throws IOException
	    {
	    	String toks[] = key.toString().split(";");
	    	t.set(toks[0], value.get());
	    	new_value.set(toks[1],value.get());
	    	output.collect(t, new_value);
	    }
	}

	
	static class reducer extends MapReduceBase implements Reducer<TextWithFreq,TextWithFreq,Text,Text>
	{
		int NUM_ENTRIES;
		
		@Override
		public void configure(JobConf job) 
		{
			super.configure(job);
			NUM_ENTRIES = Integer.parseInt(job.get("NUM_ENTRIES"));
			
		}
		Text new_key = new Text();
		Text new_value = new Text();
		@Override
	    public void reduce(TextWithFreq key, Iterator<TextWithFreq> values, OutputCollector<Text, Text> output, Reporter reporter)
	            throws IOException
	    {
			String bucket_key = key.t.toString();
			
			ArrayList<String> top_entries = new ArrayList<String>();

			while(values.hasNext() && top_entries.size() < NUM_ENTRIES)
			{
				TextWithFreq entry = values.next();
				long freq = entry.f.get();
				top_entries.add(entry.t.toString()+","+freq);
			}
			
			new_key.set(bucket_key);
			new_value.set(MiscUtils.join(top_entries, ";"));
			output.collect(new_key, new_value);
			
			
	    }
	}

	
	
	static class GroupComparator extends WritableComparator
	{

		protected GroupComparator() 
		{
			super(TextWithFreq.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) 
		{
			TextWithFreq t1 = (TextWithFreq) a;
			TextWithFreq t2 = (TextWithFreq) b;
			return t1.t.compareTo( t2.t);
		}
		
		
		
	}
	
	// boilerplate
	
	public TopNAggregator()
	{
		this(10);
	}

	public TopNAggregator(int n)
	{
		num_entries = n;	
	}
	int num_entries;
	protected void init(JobConf conf)
	{
		super.init(conf);
		conf.setMapperClass(mapper.class);
		conf.setReducerClass(reducer.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);
		conf.setMapOutputKeyClass(TextWithFreq.class);
		conf.setMapOutputValueClass(TextWithFreq.class);
		conf.set("NUM_ENTRIES", ""+num_entries);
		conf.setOutputFormat(TextOutputFormat.class);
	}
}
