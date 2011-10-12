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

public class BareCollectorServlet extends HttpServlet 
{

    private static final long serialVersionUID = 1L;

    final static Logger logger = LoggerFactory.getLogger(BareCollectorServlet.class);
    
	
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
		response.setStatus(HttpServletResponse.SC_OK);
		String payloadStr =request.getParameter("payload");
        logger.info(payloadStr);
	}
}
