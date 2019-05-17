package com.skm.applogs.udf;

import com.skm.applogs.util.DateUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: skm
 * @Date: 2019/4/14 19:08
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
@Description(name = "udf_getmonthbegin",
        value = "getweekbegin",
        extended = "getweekbegin() ;\r\n"
                + " getweekbegin(2) \r\n"
                + " getweekbegin('2019/04/14 19:12:08') \r\n"
                + " getweekbegin('2019/04/14 19:12:08',2) \r\n"
                + " getweekbegin(date_obj) \r\n"
                + " getweekbegin(date_obj,2)")
public class MonthBeginUdf extends UDF {
    /**
     * 计算现在的起始月
     * @return
     */
    public long evaluate()throws ParseException{
        return DateUtil.getMonthBeginTime(new Date()).getTime();
    }
    /**
     * 指定月偏移量
     */
    public long evaluate(int offset)throws ParseException{
        return DateUtil.getMonthBeginTime(new Date(),offset).getTime();
    }
    /**
     * 自传参
     */
    public long evaluate(Date date)throws ParseException{
        return DateUtil.getMonthBeginTime(date).getTime();
    }
    /**
     * 自传参并且指定偏移量
     */
    public long evaluate(Date date,int offset)throws ParseException{
        return DateUtil.getMonthBeginTime(date,offset).getTime();
    }
    /**
     * 自传时间串
     */
    public long evaluate(String dateStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = sdf.parse(dateStr);
        return DateUtil.getMonthBeginTime(date).getTime();
    }
    /**
     * 自传时间串并且指定偏移量
     */
    public long evaluate(String dateStr,int offset) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = sdf.parse(dateStr);
        return DateUtil.getMonthBeginTime(date).getTime();
    }
    /**
     * 自传时间串并且指定格式化格式
     */
    public long evaluate(String dateStr, String fmt) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        Date date = sdf.parse(dateStr);
        return DateUtil.getMonthBeginTime(date).getTime();
    }
    /**
     * 自传时间串并且指定格式化格式,并且指定偏移量
     */
    public long evaluate(String dateStr, String fmt,int offset) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        Date date = sdf.parse(dateStr);
        return DateUtil.getMonthBeginTime(date,offset).getTime();
    }
}
