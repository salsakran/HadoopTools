package com.whitelabellabs.hadoop.base.examples;


import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import com.whitelabellabs.hadoop.base.jobs.JobTemplate;

public class CountPlus extends JobTemplate
{
	// the "stuff"
	static class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,LongWritable>
	{
		private Text word = new Text();
		private final static LongWritable one = new LongWritable(1);

	    @Override
	    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
	            throws IOException
	    {
	    	  String line = value.toString();
	    	  StringTokenizer tokenizer = new StringTokenizer(line);
	    	  while (tokenizer.hasMoreTokens()) 
	    	  {
	    		  word.set(tokenizer.nextToken());
	    		  output.collect(word, one);
	    	  }
	    }
	}
	
	static class reducer extends MapReduceBase implements Reducer<Text,LongWritable,Text,LongWritable>
	{
		Text new_key = new Text();


		@Override
	    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter)
	            throws IOException
	    {
			int total = 0;
			while(values.hasNext())
			{
				LongWritable cur_val = values.next();
				total += cur_val.get();
			}
			
					
			output.collect(key, new LongWritable(total));

	    }
	}

	
	// boilerplate
	
	protected void init(JobConf conf)
	{
		super.init(conf);
		conf.setMapperClass(mapper.class);
		conf.setReducerClass(reducer.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(LongWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		
		conf.setInputFormat(TextInputFormat.class);
		
	}
	
}

