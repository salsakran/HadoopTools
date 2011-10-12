/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base.tests;
import junit.framework.TestCase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class MRTest extends TestCase {

	  private Mapper mapper;
	  private MapDriver driver;

	  @Before
	  public void setUp() {
	    mapper = new IdentityMapper();
	    driver = new MapDriver(mapper);
	  }

	  @Test
	  public void testIdentityMapper() {
	    driver.withInput(new Text("foo"), new Text("bar"))
	            .withOutput(new Text("foo"), new Text("bar"))
	            .runTest();
	  }
	}
