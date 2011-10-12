/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextWithFreq implements WritableComparable<TextWithFreq> 
{
	public Text t;
	public LongWritable f;

	public TextWithFreq(String s, long l)
	{
		this.t = new Text(s);
		this.f = new LongWritable(l);
		
	}

	public TextWithFreq(Text t, LongWritable l)
	{
		this.t = new Text(t);
		this.f = new LongWritable();
		this.f.set(l.get());
	}

	public TextWithFreq() 
	{
		this.t = new Text();
		this.f = new LongWritable();
	}

	public void set(String s, long l)
	{
		this.t.set(s);
		this.f.set(l);
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		t.readFields(in);
		f.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		t.write(out);
		f.write(out);
	}
	
	@Override
	public int hashCode()
	{
		return t.hashCode()*163 + f.hashCode();
		
	}

	@Override
	public boolean equals(Object o)
	{
		if (o instanceof TextWithFreq)
		{
			TextWithFreq twf = (TextWithFreq)o;
			return t.equals(twf.t) && f.equals(twf.f);
		}
		else
			return false;
	}
	
	@Override
	public int compareTo(TextWithFreq other) 
	{
		int cmp = t.compareTo(other.t);
		if(cmp == 0)
			return -f.compareTo(other.f); // NOTE: we're doing this to get topX, so this sorts in reverse order
		else
			return cmp;
	}
	
	@Override
	public String toString()
	{
			return t + ":" + f;
		
	}

}
