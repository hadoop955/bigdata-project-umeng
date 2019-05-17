package com.skm.applogs.visualize.service;

import com.skm.applogs.visualize.domain.StatBean;

/**
 * @Author: skm
 * @Date: 2019/4/16 20:12
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public interface BaseService<T> {
    public StatBean findNewUsers();
    public Long todayNewUsers(String appid);

}
