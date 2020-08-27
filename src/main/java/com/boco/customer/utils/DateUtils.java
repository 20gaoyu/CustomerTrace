package com.boco.customer.utils;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class DateUtils {

	private final static SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss 'CST' yyyy", Locale.US);

	/**
	 * 字符串转日期
	 */
	public static Timestamp str2Timestamp(String strDate) {
	    if (null == strDate) {
		return null;
	    } else {
		try {
		    SimpleDateFormat sdf3 = new SimpleDateFormat("EEE MMM dd HH:mm:ss 'CST' yyyy", Locale.US);
		    DateFormat sdf4 = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH);
		    java.util.Date date;
		    if (strDate.indexOf("GMT") >= 0) {
			date = sdf4.parse(strDate);
		    } else {
			date = sdf3.parse(strDate);
		    }
		    return new Timestamp(date.getTime());
		} catch (ParseException e) {
		    e.printStackTrace();
		    return null;
		}
	    }

	}

	/**
	 * @description UTC时间），从1970/1/1 00:00:00开始到当前的毫秒数转化成YYYYMMDDHH24MISS格式的字符串
	 * @param time
	 * @return String
	 */
	public static String paserTime(String time) {
		Date date = new Date(Long.parseLong(time.trim()));
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
		String dateString = formatter.format(date);
		return dateString;
	}
	
	public static Long transformStringToLong(String dateStr){
	    SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm");
	    SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	    SimpleDateFormat formatter3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    try {
		Date date = formatter.parse(dateStr);
		return date.getTime();
	    } catch (ParseException e) {
		try {
		    Date date = formatter2.parse(dateStr);
		    return date.getTime();
		} catch (ParseException e1) {
		    try {
			    Date date = formatter3.parse(dateStr);
			    return date.getTime();
			} catch (ParseException e2) {
			    e2.printStackTrace();
			}
			return null;
		}
	    }
	}
	
	public static void main(String[] args) {
	    System.out.println(transformStringToLong("2016/03/25 09:30:30"));
	}
	/**
	 * 按照输入的格式将utc时间转换成字符串
	 * @param time
	 * @param format
	 * @return
	 */
	public static String paserTime(String time,String format) {
		Date date = new Date(Long.parseLong(time.trim()));
		SimpleDateFormat formatter = new SimpleDateFormat(format);
		String dateString = formatter.format(date);
		return dateString;
	}	

	/**
	 * 日期格式化成字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String date2String(Date date) {
		return null != date ? sdf.format(date) : null;
	}

	/**
	 * 日期按照传入格式转换成字符串
	 * 
	 * @param date
	 * @param pattern
	 * @return
	 */
	public static String date2String(Date date, String pattern) {
		SimpleDateFormat dformat = new SimpleDateFormat(pattern);
		return dformat.format(date);
	}

	/**
	 * 将字符串日期转换成Date
	 * @param str        字符串日期
	 * @param pattern    字符串日期的格式
	 * @param locale     语言环境
	 * @return
	 */
	public static Date parse(String str, String pattern, Locale locale) {
		if (str == null || pattern == null) {
			return null;
		}
		try {
			return new SimpleDateFormat(pattern, locale).parse(str);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static List<String> getDayByWeek(int year, int week) {
	    Calendar cal = Calendar.getInstance();
	    cal.set(Calendar.YEAR, year);
	    cal.set(Calendar.WEEK_OF_YEAR, week);
	    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);

	    List<String> list = new ArrayList<String>();
	    for (int i = 1; i <= 7; i++) {
		if (year == cal.get(Calendar.YEAR)) {
		    list.add(date2String(cal.getTime(), "yyyyMMdd"));
		}
		cal.add(Calendar.DAY_OF_YEAR,1);
	    }
	    return list;
	}
}
