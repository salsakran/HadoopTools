/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base.tests;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.whitelabellabs.hadoop.base.DateUtils;

import junit.framework.TestCase;

	public class DateUtilsTest extends TestCase
  	{
  		public void testStartEnd() throws ParseException
  		{
  			String start = "2010_01_01";
  			String end = "2010_01_01";
  			
  			List<String> results = DateUtils.getDates(start, end);
  			assertEquals(1, results.size());
  			assertEquals(results.get(0),  "2010_01_01");
  			
  			
  			start = "2010_01_01";
  			end = "2010_01_03";
  			
  			results = DateUtils.getDates(start, end);
  			assertEquals(3, results.size());
  			assertEquals(results.get(0),  "2010_01_01");
  			assertEquals(results.get(1),  "2010_01_02");
  			assertEquals(results.get(2),  "2010_01_03");

  			
  		}
  		
  		public void testTest()
  		{
  			List<String> a = new ArrayList<String>();
  			for(int i =0 ; i < 5; i++)
  				a.add(""+i);
  			assertEquals(4, a.subList(0,a.size()-1).size());
  		}
  		
  	}
