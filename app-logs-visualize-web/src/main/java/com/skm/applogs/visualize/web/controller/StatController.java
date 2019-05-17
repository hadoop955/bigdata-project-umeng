package com.skm.applogs.visualize.web.controller;

import com.skm.applogs.visualize.domain.StatBean;
import com.skm.applogs.visualize.service.StatService;
import org.apache.zookeeper.data.Stat;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;


import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 统计分析类
 */
@Controller
@RequestMapping("/stat")
public class StatController {

    @Resource(name = "statService")
    public StatService ss;

    /**
     * appid = "sdk34734"
     * 本周每天新增用户数
     */
    @RequestMapping("/newusers")
    public String findNewUsers() {
        StatBean bean = ss.findNewUsers();
        System.out.println(bean.getCount());
        return "index";
    }

    @RequestMapping("/index")
    public String toStatPage() {
        return "index";
    }

    @RequestMapping("/weeks")
    public String findDayNewUsersInWeek() {
        ss.findDayNewUsersInWeek("sdk34734");
        return "index";
    }

    @RequestMapping("/stat1")
    @ResponseBody
    public StatBean statTest() {
        StatBean statBean = new StatBean();
        statBean.setDate("2019/04/20");
        statBean.setCount(6666);
        return statBean;
    }

    @RequestMapping("stat2")
    @ResponseBody
    public List<StatBean> statTest2() {
        List<StatBean> list = new ArrayList<StatBean>();
        for (int i = 0; i < 10; i++) {
            StatBean statBean = new StatBean();
            statBean.setDate("2019/04" + i);
            statBean.setCount(121 + i);
            list.add(statBean);
        }
        return list;
    }

    @RequestMapping("/findThisWeekNewUsers")
    @ResponseBody
    public Map<String,Object> findThisWeekNewUsers() {
        List<StatBean> list = ss.findThisWeekNewUsers("sdk34734");
        Map<String, Object> map = new HashMap<String, Object>();

        String[] xlables = new String[list.size()];
        long[] newUsers = new long[list.size()];
        for (int i = 0; i < list.size(); i++) {
            xlables[i] = list.get(i).getDate();
            newUsers[i] = list.get(i).getCount();
        }
        map.put("lables", xlables);
        map.put("data", newUsers);
        return map;
    }

}
