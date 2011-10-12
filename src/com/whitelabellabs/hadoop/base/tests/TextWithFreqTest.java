/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base.tests;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;

import com.whitelabellabs.hadoop.base.TextWithFreq;

import junit.framework.TestCase;


public class TextWithFreqTest extends TestCase
{
	
	public void testSort()
	{
		TextWithFreq t1 = new TextWithFreq("10asdg", 1);
		TextWithFreq t2 = new TextWithFreq("10asdg", 3);
		TextWithFreq t3 = new TextWithFreq("10asdg", 2);
		TextWithFreq t4 = new TextWithFreq("10as", 1);
		TextWithFreq t5 = new TextWithFreq("10dg", 1);
		
		ArrayList<TextWithFreq> a = new ArrayList<TextWithFreq>();
		a.add(t1);
		a.add(t2);
		a.add(t3);
		a.add(t4);
		a.add(t5);
		Collections.sort(a);
		
		assertEquals(a.get(0), t4);
		assertEquals(a.get(1), t2);
		assertEquals(a.get(2), t3);
		assertEquals(a.get(3), t1);
		assertEquals(a.get(4), t5);

	}
	
	public void testEquality()
	{
		TextWithFreq t1 = new TextWithFreq("10asdg", 1);
		TextWithFreq t2 = new TextWithFreq("10asdg", 3);
		TextWithFreq t3 = new TextWithFreq("10asdg", 1);
		TextWithFreq t4 = new TextWithFreq("10as", 1);
		TextWithFreq t5 = new TextWithFreq("10dg", 1);
		
		assertEquals(true, t1.equals(t3));
		assertEquals(false, t1.equals(t2));
		assertEquals(false, t1.equals(t4));
		assertEquals(false, t1.equals(t5));
		assertEquals(false, t4.equals(t5));
	
	}
	
	
}
