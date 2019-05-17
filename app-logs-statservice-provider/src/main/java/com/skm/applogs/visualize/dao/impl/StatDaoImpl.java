package com.skm.applogs.visualize.dao.impl;

import com.skm.applogs.visualize.dao.BaseDao;
import com.skm.applogs.visualize.domain.StatBean;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.support.SqlSessionDaoSupport;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.List;


/**
 * 统计
 */
@Repository("statDao")
public class StatDaoImpl extends SqlSessionDaoSupport implements BaseDao {

	/**
	 * 查询新增用户
	 */
	public StatBean findNewUsers(){
		return getSqlSession().selectOne("stats.newusers");
	}

	@Resource
	public void setSqlSessionFactory(SqlSessionFactory sqlSessionFactory) {
		super.setSqlSessionFactory(sqlSessionFactory);
	}

	//查询指定app的今日新增用户数
	public Long todayNewUsers(String appid){
		return getSqlSession().selectOne("stats.selectAppIdTodayNewusers", appid);
	}

	public List<StatBean> findDayNewUsersInWeek(String appid) {
		return getSqlSession().selectList("stats.selectThisWeekNewusers");
	}

	public List<StatBean> findThisWeekNewUsers(String appid) {
		return getSqlSession().selectList("stats.selectThisWeekNewusers",appid);
	}


}
