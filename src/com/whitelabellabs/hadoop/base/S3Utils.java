/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.tools.DistCp;


public class S3Utils 
{
	public static void copyToS3(String original_location, String target_location) throws Exception
	{
		JobConf conf = new JobConf(S3Utils.class);
		String[] locations = new String[3];
		locations[0] = "-update";
		locations[1] = original_location;
		locations[2] = target_location;
		new DistCp(conf).run(locations);
		
	}
	
	public static void copyToS3withOverwrite(String original_location, String target_location) throws Exception
	{
		JobConf conf = new JobConf(S3Utils.class);
		String[] locations = new String[3];
		locations[0] = "-overwrite";
		locations[1] = original_location;
		locations[2] = target_location;
		new DistCp(conf).run(locations);
		
	}
}
