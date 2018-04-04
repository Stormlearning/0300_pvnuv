package com.ymd.learn.uv;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
	
	public static String toDate(String datetime) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date date = sdf.parse(datetime);
			
			SimpleDateFormat dateSdf = new SimpleDateFormat("yyyy-MM-dd");
			return dateSdf.format(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	
}
