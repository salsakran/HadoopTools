package com.whitelabellabs.collector;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.servlet.ServletException;
import javax.servlet.UnavailableException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectorServelet extends HttpServlet 
{
	private static final long serialVersionUID = 1L;

    final static Logger logger = LoggerFactory.getLogger(CollectorServelet.class);
    final static String DEVICE_ID_KEY = "UUID";
    
    protected MessageDigest algorithm;
    
    
	public void init() throws ServletException 
	{
		super.init();
		try 
		{
			algorithm = MessageDigest.getInstance("MD5");
		} 
		catch (NoSuchAlgorithmException e) 
		{
			throw new UnavailableException("Couldn't instantiate message digest algorith.");
		}
		
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException 
	{
		
		response.setStatus(HttpServletResponse.SC_OK);
		String payloadStr =request.getParameter("payload");
		JSONObject p = null;

        try 
        {
            p = (JSONObject) JSONValue.parse(payloadStr);
            String device_id = (String)p.get(DEVICE_ID_KEY);
        	p.put(DEVICE_ID_KEY, hashValue(device_id.getBytes()));
            logger.info(p.toJSONString());
        } 
        catch(NoSuchAlgorithmException ex)
        {}
        catch(Exception ex) 
        {

        }
	}
	
	protected void anonymizeField(JSONObject jo, String field_spec)
	{
		String[] specs = field_spec.split(".");
		
		
		
	}
	
	protected String hashValue(byte[] b) throws NoSuchAlgorithmException
	{
		
    	algorithm.reset();
    	algorithm.update(b);
    	byte messageDigest[] = algorithm.digest();
                
    	StringBuffer result = new StringBuffer();
    	for (int i=0;i<messageDigest.length;i++) {
    		result.append(Integer.toHexString(0xFF & messageDigest[i]));
    	}
    	return result.toString();
		
	}
}
