/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base.status;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class StatusUtils 
{
	URL status_server_url;
	private static StatusUtils single_me = new StatusUtils();
	
	private StatusUtils()
	{}
	
	static public StatusUtils getStatusHandler()
	{
		return single_me;
	}
	
	public void set_url(String status_server_url)
	{
		try 
		{
			this.status_server_url = new URL(status_server_url);
		} 
		catch (MalformedURLException e) 
		{
			e.printStackTrace();
		}
	}
	
	public void jobStarted(String RecipeID)
	{
		updateStatus("started", RecipeID);
	}
	
	public void jobFinished(String RecipeID)
	{
		updateStatus("finished", RecipeID);
	}
	
	public void jobFailed(String RecipeID)
	{
		updateStatus("failed", RecipeID);
	}

	public void sendCounters(String RecipeID)
	{}
	
	
	public void updateStatus(String status, String RecipeID)
	{
		if(status_server_url == null)
			return;
		 try 
		 {
			URLConnection uc = status_server_url.openConnection( );
			uc.setDoOutput(true);
			OutputStream raw = uc.getOutputStream( );
			OutputStream buff = new BufferedOutputStream(raw);
			OutputStreamWriter out = new OutputStreamWriter(buff, "8859_1");
			out.write("status=");
			out.write(status);
			out.write("&recipe=");
			out.write(RecipeID);
			out.flush( );
			out.close( );
		 } 
		 catch (IOException e) 
		 {
			e.printStackTrace();
		}
		 

		
	}
}
