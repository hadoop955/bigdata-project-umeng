package com.skm.applogs.visualize;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by Administrator on 2017/6/29.
 */
public class ServiceStarter {
	public static void main(String[] args) throws InterruptedException {
		ApplicationContext ac = new ClassPathXmlApplicationContext("beans.xml");
		while (true) {
			Thread.sleep(1000);
		}
	}
}
