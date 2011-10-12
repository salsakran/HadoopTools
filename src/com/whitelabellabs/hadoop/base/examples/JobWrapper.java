package com.whitelabellabs.hadoop.base.examples;


public class JobWrapper {

	static public void main(String args[]) throws Exception
	{
		// create a string representing the inputs
		// input should be of the form "year/month/day" 
		String input = args[0];
		String output = args[1];
		
		new CountPlus().run(input, output +"_" +  System.currentTimeMillis());

	}

}
