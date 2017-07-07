package com.quickutil.platform;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * @author shijie.ruan
 */
public class DateTimeUtil {
	private static final DateFormat yearMonthDayDateFormat = new SimpleDateFormat("yyyy-MM-dd");

	/**
	 * 获取昨天的日期
	 * 
	 * @param timePattenStr
	 * @return
	 */
	public static String getYesterdayDate(String timePattenStr) {
		DateFormat dateFormat = new SimpleDateFormat(timePattenStr);
		final Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		Date date = cal.getTime();
		return dateFormat.format(date);
	}

	/**
	 * 计算两个日期的差
	 * 
	 * @param d1
	 * @param d2
	 * @return
	 */
	public static long getDiffDays(Date d1, Date d2) {
		long diff = Math.abs(d1.getTime() - d2.getTime());
		return (diff + 12 * 60 * 60 * 1000) / (24 * 60 * 60 * 1000);
	}

	/**
	 * 根据时间范围获取过去的日期,比如 timeRange = 7,则获取过去一周的日期
	 * 
	 * @param timeRange
	 * @return
	 */
	public static String[] getDateFromNow(String timePattenStr, int timeRange) {
		String[] result = new String[timeRange];
		DateFormat dateFormat = new SimpleDateFormat(timePattenStr);
		final Calendar cal = Calendar.getInstance();
		for (int i = 0; i < timeRange; i++) {
			cal.add(Calendar.DATE, -1);
			Date date = cal.getTime();
			result[i] = dateFormat.format(date);
		}
		return result;
	}

	/**
	 * 获取 startAt 和 endAt 之间的日期,输入和输出的日期格式都必须是 yyyy-MM-dd
	 * 
	 * @param startAt
	 * @param endAt
	 * @return
	 */
	public static String[] getDateBetweenRange(String startAt, String endAt) throws ParseException {
		Date start = yearMonthDayDateFormat.parse(startAt);
		Date end = yearMonthDayDateFormat.parse(endAt);

		List<String> result = new LinkedList<>();
		final Calendar cal = Calendar.getInstance();
		cal.setTime(start);
		long timeRange = getDiffDays(start, end);
		Date date = cal.getTime();
		result.add(yearMonthDayDateFormat.format(date));
		for (int i = 0; i < timeRange; i++) {
			cal.add(Calendar.DATE, 1);
			date = cal.getTime();
			result.add(yearMonthDayDateFormat.format(date));
		}
		return result.toArray(new String[0]);
	}

}
