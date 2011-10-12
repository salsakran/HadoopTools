/*******************************************************************************
 * Copyright 2010 WhiteLabelLabs
 *   Not for redistribution without written permission.
 ******************************************************************************/
package com.whitelabellabs.hadoop.base;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


public class DateUtils 
{
	public static DateFormat date_format = new SimpleDateFormat("yyyy_MM_dd");
	static List<String> getDates(Calendar first_day, Calendar final_day)
	{
		List<String> dates = new ArrayList<String>();
		
		Calendar current_date = (Calendar) first_day.clone();
	    while (current_date.before(final_day)) 
	    {
	    	dates.add(get8CharDateString(current_date.getTime()));
	        current_date.add(Calendar.DATE, 1);
	    }
	    dates.add(get8CharDateString(final_day.getTime()));
	    
		return dates;
		
	}
	

	public static List<String> getDates(String start_date, String end_date) throws ParseException
	{
		Calendar first_day = getCalendarForDate(start_date);
		Calendar final_day = getCalendarForDate(end_date);
		
		return getDates(first_day,final_day);
		
	}
	


	public static List<String> getDatesFromStart(String start_date, int num_days) throws ParseException
	{
		Calendar first_day = getCalendarForDate(start_date);
		Calendar final_day = (Calendar) first_day.clone();
		final_day.add(Calendar.DATE, (num_days - 1));
		
		return getDates(first_day,final_day);
		
	}
	
	public static List<String> getPriorMonths(String end_date, int num_months ) throws ParseException
	{
		Calendar first_day = getCalendarForDate(end_date);
		Calendar final_day = (Calendar) first_day.clone();
		final_day.add(Calendar.MONTH, (num_months - 1));
		
		return getDates(first_day,final_day);
		
	}
	
	public static List<String> getDatesFromEnd(String end_date, int num_days) throws ParseException
	{
		
		Calendar final_day = getCalendarForDate(end_date);
		Calendar first_day = (Calendar) final_day.clone();
		first_day.add(Calendar.DATE, -(num_days - 1));
		
		
		return getDates(first_day,final_day);
		
	}
	
	


	  	
	  	private static String get8CharDateString(Date time)
	  	{
	  		return date_format.format(time);
	  		
	  	}


	      
	  	private static Calendar getCalendarForDate(String endDate) throws ParseException 
	  	{
	  		Date d= date_format.parse(endDate);
	  		Calendar cal=Calendar.getInstance();
	  		cal.setTime(d);
			return cal;
		}
	  	
	
}
