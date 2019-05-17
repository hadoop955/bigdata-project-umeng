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
@Description(name = "udf_formattime",
        value = "formattime",
        extended = "formattime() ;\r\n"
                + "formattime(1233432,'yyyy/MM/01') \r\n"
                + "formattime(1233432,'yyyy/MM/dd')")
public class FormatTimeUdf extends UDF {
    /**
     * 格式化时间类型，long类型
     *
     * @param ms
     * @param fmt
     * @return
     */
    public String evaluate(long ms, String fmt)throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(fmt);
        Date date = new Date();
        date.setTime(ms);
        return simpleDateFormat.format(date);
    }

    /**
     * 格式化时间类型，long类型
     * @param ms
     * @param fmt
     * @return
     */
    public String evaluate(String ms, String fmt)throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(fmt);
        Date date = new Date();
        date.setTime(Long.parseLong(ms));
        return simpleDateFormat.format(date);
    }

    /**
     * 格式化类型时间，按照周
     * 此处的week参数用来区分函数
     * @param ms
     * @param fmt
     * @param week
     * @return
     * @throws ParseException
     */
    public String evaluate(long ms, String fmt, int week) throws ParseException {
        Date date = new Date();
        date.setTime(ms);
        //周内第一天
        Date firstDay = DateUtil.getWeekBeginTime(date);
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        return sdf.format(firstDay);
    }
}