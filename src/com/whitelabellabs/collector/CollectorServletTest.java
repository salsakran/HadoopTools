package com.whitelabellabs.collector;

import static org.junit.Assert.*;

import java.security.NoSuchAlgorithmException;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CollectorServletTest 
{

	String test_payload = "{ \"ver\" : \"1\", \"type\" : \"hits\", \"seq\" : \"0\", \"hits\" : [{\"ts\" : \"1276730926\", \"rt\" : \"5500\", \"cfq\" : \"2432\", \"sig\" : \"-86\", \"ant\" : \"1\", \"suid\" : \"QzQ6MEE6RDY6RTQ6MTI6NjA=\"},{\"ts\" : \"1276730927\", \"rt\" : \"5500\", \"cfq\" : \"2432\", \"sig\" : \"-83\", \"ant\" : \"1\", \"suid\" : \"OUQ6QzM6REU6NTc6RjE6NDg=\", \"auid\" : \"M0E6OTQ6Q0Y6N0Q6Qjk6MjI=\"},{\"ts\" : \"1276730927\", \"rt\" : \"5500\", \"cfq\" : \"2432\", \"sig\" : \"-83\", \"ant\" : \"1\", \"suid\" : \"OUQ6QzM6REU6NTc6RjE6NDg=\", \"auid\" : \"M0E6OTQ6Q0Y6N0Q6Qjk6MjI=\"},{\"ts\" : \"1276730928\", \"rt\" : \"5500\", \"cfq\" : \"2432\", \"sig\" : \"-84\", \"ant\" : \"1\", \"suid\" : \"MDg6OTQ6M0I6ODg6ODI6NTc=\"},{\"ts\" : \"1276730928\", \"rt\" : \"5500\", \"cfq\" : \"2432\", \"sig\" : \"-86\", \"ant\" : \"1\", \"suid\" : \"OEM6OEI6QzA6OUU6MzQ6MUE=\", \"auid\" : \"RUQ6NDE6N0M6MkI6RkU6RTI=\"},{\"ts\" : \"1276730928\", \"rt\" : \"5500\", \"cfq\" : \"2432\", \"sig\" : \"-85\", \"ant\" : \"1\", \"suid\" : \"MUU6OTU6Mjg6MzI6NTc6QjY=\", \"auid\" : \"NDk6RTM6Rjc6QjA6MEE6QjI=\"},{\"ts\" : \"1276730928\", \"rt\" : \"5500\", \"cfq\" : \"2432\", \"sig\" : \"-85\", \"ant\" : \"1\", \"suid\" : \"Q0Q6OEU6MDM6REI6QkI6QUQ=\", \"auid\" : \"MzA6QjU6QkQ6RDg6ODA6MTE=\"},{\"ts\" : \"1276730928\", \"rt\" : \"5500\", \"cfq\" : \"2432\", \"sig\" : \"-83\", \"ant\" : \"1\", \"suid\" : \"ODM6MTA6MDA6MTM6NDI6QTQ=\"},{\"ts\" : \"1276730928\", \"rt\" : \"5500\", \"cfq\" : \"2432\", \"sig\" : \"-82\", \"ant\" : \"1\", \"suid\" : \"NDY6RUQ6OUM6QzY6MDA6MDA=\", \"auid\" : \"MDA6MDA6MDA6MDA6MDA6MDA=\"},{\"ts\" : \"1276730928\", \"rt\" : \"5500\", \"cfq\" : \"2432\", \"sig\" : \"-83\", \"ant\" : \"1\", \"suid\" : \"NkQ6Mjg6MUI6MEM6MDA6MDA=\"}]} "; 
	String test_hit = "{\"ts\" : \"1276730928\", \"rt\" : \"5500\", \"cfq\" : \"2432\", \"sig\" : \"-83\", \"ant\" : \"1\", \"suid\" : \"NkQ6Mjg6MUI6MEM6MDA6MDA=\"}";
	String test_mac = "NkQ6Mjg6MUI6MEM6MDA6MDA=";
	String expected_mac = "1fcb28da7f6dcf307b0b90893e0d43ba";
	
	@Test
	public void testProcessPayload()
	{
		// this is just to step through for now
		new CollectorServlet().process_payload(test_payload, new String[]{"suid"});
	}

	
	
	@Test
	public void testAnonymizeField() 
	{
		JSONObject hit = (JSONObject) JSONValue.parse(test_hit);
		try 
		{
			new CollectorServlet().anonymizeField(hit, "suid");
			assertEquals(hit.get("suid"), expected_mac);
		}
		catch (NoSuchAlgorithmException e) 
		{
			fail("can't instantiate message digester");
		}
	}

	@Test
	public void testHashValue() 
	{
		try
		{
			String hashed_value =new CollectorServlet().hashValue(test_mac.getBytes()); 
			assertTrue(hashed_value.equals(expected_mac));
		} 
		catch (NoSuchAlgorithmException e) 
		{
			fail("can't instantiate message digester");	
		}
	}

}
