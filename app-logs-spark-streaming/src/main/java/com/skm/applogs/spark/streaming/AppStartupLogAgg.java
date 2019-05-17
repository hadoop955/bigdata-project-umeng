package com.skm.applogs.spark.streaming;

import com.skm.app.common.AppStartupLog;

import java.io.Serializable;

/**
 * AppStartupLog聚合类
 */
public class AppStartupLogAgg implements Serializable {
    private AppStartupLog log;
    //首次启动时间
    private long firstTime;
    //最后访问时间
    private long lastTime;

    public AppStartupLog getLog() {
        return log;
    }

    public void setLog(AppStartupLog log) {
        this.log = log;
    }

    public long getFirstTime() {
        return firstTime;
    }

    public void setFirstTime(long firstTime) {
        this.firstTime = firstTime;
    }

    public long getLastTime() {
        return lastTime;
    }

    public void setLastTime(long lastTime) {
        this.lastTime = lastTime;
    }
}
