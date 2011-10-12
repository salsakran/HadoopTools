package com.whitelabellabs.collector;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HardcodedCollectorServlet extends HttpServlet
{


	private static final long serialVersionUID = 1L;

    final static Logger logger = LoggerFactory.getLogger(HardcodedCollectorServlet.class);
    
    
    
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException 
	{
		process_request(request, response);
	}
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException 
	{
		process_request(request, response);
	}
	
	protected void process_request(HttpServletRequest request, HttpServletResponse response)
	{
		
		String payloadStr =request.getParameter("payload");
		String fields_str = request.getParameter("anonymize");

		if(payloadStr == null)
			return;
		//String[] fields_to_anonymize = new String[] {"auid","suid"};
		String[] fields_to_anonymize;
		if (fields_str != null)
			fields_to_anonymize = fields_str.split(",");
		else
			fields_to_anonymize = new String[0];
		process_payload(payloadStr, fields_to_anonymize);
		response.setStatus(HttpServletResponse.SC_OK);
	}

	public void process_payload(String payloadStr, String[] fields_to_anonymize)
	{
		
		try 
        {
        	JSONObject p = (JSONObject) JSONValue.parse(payloadStr);
            JSONArray hits = (JSONArray) p.get("hits");
            JSONObject header = (JSONObject) p.clone();
            header.remove("hits");
            for(Object hit : hits)
            {
            	try
            	{
	            	if (hit instanceof JSONObject) 
	            	{
	            		JSONObject hit_o = (JSONObject) hit;
	            		for(String field_to_anonymize : fields_to_anonymize) 
	            			anonymizeField(hit_o, field_to_anonymize);
	            		hit_o.put("header", header);
	            		logger.info(hit_o.toJSONString());
	            	}
	            	else
	            	{
	            		// this should not happen, but ....yeah.
	            		logger.info(hit.toString() + "header:" + header.toJSONString());
	            	}
	            		
            	}
            	catch(Exception e)
            	{
            		// skip parse errors
            	}
            }
        	
          
        } 
        catch(Exception ex) 
        {

        }
	}
	
	public void anonymizeField(JSONObject hit, String field_key) throws NoSuchAlgorithmException
	{
		String field = hit.get(field_key).toString();
		hit.put(field_key, hashValue(field.getBytes()));
		
		
	}
	
	public String hashValue(byte[] b) throws NoSuchAlgorithmException
	{
		MessageDigest algorithm = MessageDigest.getInstance("MD5");		
    	algorithm.reset();
    	algorithm.update(b);
    	byte messageDigest[] = algorithm.digest();
                
    	StringBuffer result = new StringBuffer();
    	for (int i=0;i<messageDigest.length;i++) {
    		result.append(String.format("%02x", messageDigest[i]));
    	}
    	return result.toString();
		
	}
	
}
