package org.shirdrn.streaming.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTimeUtils {
	
	public static String format(Date date, String format) {
		DateFormat df = new SimpleDateFormat(format);
		return df.format(date);
	}
	
	public static String format(long timestamp, String format) {
		Date date = new Date(timestamp);
		return format(date, format);
	}
	
}
