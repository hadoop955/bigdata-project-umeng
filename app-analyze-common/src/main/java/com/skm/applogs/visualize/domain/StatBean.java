package com.skm.applogs.visualize.domain;

import java.io.Serializable;

/**
 * @Author: skm
 * @Date: 2019/4/16 16:01
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public class StatBean implements Serializable {
    //统计日期
    private String date;
    //统计数量
    private long count;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
