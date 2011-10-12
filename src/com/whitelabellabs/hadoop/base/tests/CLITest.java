package com.whitelabellabs.hadoop.base.tests;

import static org.junit.Assert.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.junit.Test;
import com.whitelabellabs.hadoop.base.rollup.RollupWrapperCLI;;
public class CLITest {

	@Test
	public void testStartEnd() throws ParseException {
		String[] test_args = {"-s", "20111005", "-e","20111006"};
		CommandLine cl = RollupWrapperCLI.parse(test_args);
		assertTrue(cl.hasOption('s'));
		assertTrue(cl.hasOption('e'));
		assertTrue(cl.getOptionValue('s').equalsIgnoreCase("20111005"));
		assertTrue(cl.getOptionValue('e').equalsIgnoreCase("20111006"));
	}

	@Test
	public void testEndDays() throws ParseException {
		String[] test_args = {"-n", "1", "-e","20111006"};
		CommandLine cl = RollupWrapperCLI.parse(test_args);
		assertTrue(cl.hasOption('n'));
		assertTrue(cl.hasOption('e'));
		assertTrue(cl.getOptionValue('n').equalsIgnoreCase("1"));
		assertTrue(cl.getOptionValue('e').equalsIgnoreCase("20111006"));
	}

	@Test
	public void testLongForm() throws ParseException {
		String[] test_args = {"--num_days", "1", "--end_date","20111006"};
		CommandLine cl = RollupWrapperCLI.parse(test_args);
		assertTrue(cl.hasOption('n'));
		assertTrue(cl.hasOption('e'));
		assertTrue(cl.getOptionValue('n').equalsIgnoreCase("1"));
		assertTrue(cl.getOptionValue('e').equalsIgnoreCase("20111006"));
	}

}
