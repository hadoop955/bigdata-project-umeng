import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @Author: skm
 * @Date: 2019/4/13 16:38
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public class TestHive {
    /**
     * 计算某天的起始时刻（毫秒值）
     *
     * @throws ParseException
     */
    @Test
    public void testStartTime() throws ParseException {
        Date d = new Date();
        long ms = getZeroDate(d).getTime();
        System.out.println(ms);
    }

    @Test
    public void testEndTime() throws ParseException {
        Date date = new Date();
        Date zeroTime = getZeroDate(date);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(zeroTime);

        calendar.add(Calendar.DAY_OF_MONTH, 1);
        long endDate = calendar.getTimeInMillis();
        System.out.println(endDate);
    }

    /**
     * 获得当前时间起始周的日期
     */
    @Test
    public void testWeekTime() {
        Date date = new Date();
        Date zeroTime = getZeroDate(date);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(zeroTime);
        int n = calendar.get(Calendar.DAY_OF_WEEK);
        //日历向前翻几天
        calendar.add(Calendar.DAY_OF_MONTH, -(n - 1));
//        Date a = calendar.getTime();
//        System.out.println(a);
        long startTime = calendar.getTimeInMillis();
        System.out.println(startTime);
    }

    /**
     * 获得当前时间本周的最后一天的时间
     */
    @Test
    public void testWeekEndTime() {
        Date date = new Date();
        Date zeroTime = getZeroDate(date);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(zeroTime);

        int n = calendar.get(Calendar.DAY_OF_WEEK);
        calendar.add(Calendar.DAY_OF_MONTH, (8 - n));
//        System.out.println(calendar.getTime());
        long ms = calendar.getTimeInMillis();
        System.out.println(ms);

    }

    /**
     * 得到一个月的起始天的日期
     */
    @Test
    public void testMonthTime() throws ParseException {
        Date date = new Date();
        Date zeroTime = getZeroDate(date);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/01 00:00:00");
        long ms = sdf.parse(sdf.format(zeroTime)).getTime();
        System.out.println(ms);
    }

    /**
     * 得到一个月的结束天的日期
     */
    @Test
    public void testEndMonthTime() throws ParseException {
        Date date = new Date();
        Date zeroTime = getZeroDate(date);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/01 00:00:00");

        Date firstDay = sdf.parse(sdf.format(zeroTime));

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(firstDay);

        calendar.add(Calendar.MONTH, 1);
        System.out.println(calendar.getTime());
        long ms = calendar.getTimeInMillis();
        System.out.println(ms);
    }

    /**
     * 得到指定时间的00:00时刻
     *
     * @param date
     * @return
     */
    private Date getZeroDate(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd 00:00:00");
        try {
            return sdf.parse(sdf.format(date));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    @Test
    public void test1(){
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd 00:00:00");
        System.out.println(simpleDateFormat.format(date));
    }
}
