package com.skm.applogs.udf;

import com.skm.applogs.util.DateUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: skm
 * @Date: 2019/4/14 18:35
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
@Description(name = "udf_getdaybegin",
        value = "getdaybegin",
        extended = "getdaybegin() ;\r\n"
                + " getdaybegin(2) \r\n"
                + " getdaybegin('2019/04/14 19:12:08') \r\n"
                + " getdaybegin('2019/04/14 19:12:08',2) \r\n"
                + " getdaybegin(date_obj) \r\n"
                + " getdaybegin(date_obj,2)")
public class DayBeginUdf extends UDF {
    /**
     * 计算现在的起始时刻（毫秒数）
     */
    public long evaluate()throws ParseException{
        return evaluate(new Date());
    }

    /**
     * 计算指定偏移量
    */
    public long evaluate(int offset)throws ParseException{
        return evaluate(DateUtil.getBeginTime(new Date(),offset));
    }

    /**
     * 计算某天的结束时刻
     * @param date
     * @return
     */
    public static long evaluate(Date date)throws ParseException {
        return DateUtil.getBeginTime(date).getTime();
    }

    /**
     * 指定天的偏移量
     */
    public long evaluate(Date date, int offset)throws ParseException {
        return DateUtil.getBeginTime(date, offset).getTime();
    }
    /**
     *计算某天的起始时刻
     */
    public long evaluate(String dataStr) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = simpleDateFormat.parse(dataStr);
        return evaluate(date);
    }
    /**
     *计算某天的起始时刻(带偏移量)
     */
    public long evaluate(String dataStr,int offset) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = simpleDateFormat.parse(dataStr);
        return DateUtil.getBeginTime(date,offset).getTime();
    }
    /**
     * 计算某天的起始时刻(毫秒数)
     */
    public long evaluate(String dateStr, String fmt) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        Date date = sdf.parse(dateStr);
        return DateUtil.getBeginTime(date).getTime();
    }
    /**
     * 计算某天的起始时刻(毫秒数)
     */
    public long evaluate(String dateStr, String fmt,int offset) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        Date date = sdf.parse(dateStr);
        return DateUtil.getBeginTime(date,offset).getTime();
    }
}
