package com.skm.applogs.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @Author: skm
 * @Date: 2019/4/14 9:37
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public class DateUtil {
    /**
     * 得到指定天的零时刻
     *
     * @param date
     * @return
     */
    public static Date getBeginTime(Date date) {
        SimpleDateFormat sdf = null;
        try {
            sdf = new SimpleDateFormat("yyyy/MM/dd 00:00:00");
            return sdf.parse(sdf.format(date));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 得到指定天的偏移量时刻（正表示后推几天，负偏移量表示前几天）
     */
    public static Date getBeginTime(Date date, int offset) {
        Date beginTime = null;
        try {
            beginTime = getBeginTime(date);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(beginTime);
            calendar.add(Calendar.DAY_OF_MONTH, offset);
            return calendar.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 得到指定天所在周的起始时刻
     */
    public static Date getWeekBeginTime(Date date) {
        Date beginTime = null;
        try {
            beginTime = getBeginTime(date);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(beginTime);
            int n = calendar.get(Calendar.DAY_OF_WEEK);
            calendar.add(Calendar.DAY_OF_MONTH, -(n - 1));
            return calendar.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 得到指定天的偏移量所在周的起始时刻
     */
    public static Date getWeekBeginTime(Date date, int offset) {
        Calendar calendar = null;
        try {
            calendar = Calendar.getInstance();
            calendar.setTime(getWeekBeginTime(date));
            calendar.add(Calendar.DAY_OF_MONTH, offset * 7);
            return calendar.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 得到指定天所在月的起始时刻
     */
    public static Date getMonthBeginTime(Date date) {
        Date beginTime = null;
        SimpleDateFormat sdf = null;
        try {
            beginTime = getBeginTime(date);
            sdf = new SimpleDateFormat("yyyy/MM/01 00:00:00");
            return sdf.parse(sdf.format(beginTime));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 得到指定天所在月的偏移量时刻
     */
    public static Date getMonthBeginTime(Date date, int offset) {
        Date beginTime = null;
        Calendar calendar = null;
        try {
            beginTime = new Date();
            calendar = Calendar.getInstance();
            calendar.setTime(getMonthBeginTime(beginTime));
            calendar.add(Calendar.MONTH, offset);
            return calendar.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
