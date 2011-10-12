/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base;

public class Histogram
{
	static final long[] bins ={1,2,5,10,25,50,75,100,250,500,1000,1500,2500,5000,7500,10000,25000,50000,100000,200000,500000,1000000}; 
	public static String getBinTagForLong(long x)
	{
		boolean negative = false;
		long value = x;
		if(x < 0)
		{
			negative = true;
			value = -x;
		}
		
		int index = 0;
		while(index < bins.length && bins[index] < value)
		{
			index++;
		}
		String result = "";
		if(negative)
			result += "-";
		if(index >= bins.length)
			result += "HUGE";
		else
			result += bins[index];
		return result;
	}
	public static void main(String args[])
	{
		
		long x = Long.parseLong(args[0]);
		System.out.println(getBinTagForLong(x));
	}
}
