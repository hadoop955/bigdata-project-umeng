package com.skm.applogs.visualize.service;

import com.skm.applogs.visualize.domain.StatBean;

import java.util.List;

/**
 * @Author: skm
 * @Date: 2019/4/16 15:22
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public interface StatService extends BaseService<StatBean> {
    public StatBean findNewUsers();

    public Long todayNewUsers(String appid);

    public List<StatBean> findDayNewUsersInWeek(String appid);

    public List<StatBean> findThisWeekNewUsers(String appid);
}
