package com.skm.applogs.visualize.dao;

import com.skm.applogs.visualize.domain.StatBean;

import java.util.List;

/**
 * BaseDao接口
 */
public interface BaseDao<T> {
    public StatBean findNewUsers();

    //查询指定app的今日新增用户数
    public Long todayNewUsers(String appid);

    public List<StatBean> findDayNewUsersInWeek(String appid);

    public List<StatBean> findThisWeekNewUsers(String appid);
}
