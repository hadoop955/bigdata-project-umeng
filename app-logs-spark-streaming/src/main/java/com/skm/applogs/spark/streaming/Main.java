package com.skm.applogs.spark.streaming;

import com.alibaba.fastjson.JSONObject;
import com.skm.app.common.AppStartupLog;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.sql.*;
import java.util.*;


/**
 * 入口程序
 */
public class Main {
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf();
		conf.setAppName("App-Logs-Spark-Streaming");
		conf.setMaster("local[3]") ;

		//创建流上下文
		JavaStreamingContext ssc = new JavaStreamingContext(conf, Seconds.apply(5));

		//kafka属性
		Map<String,Object> kafkaProp = new HashMap<String, Object>();
		kafkaProp.put("bootstrap.servers", "master:9092,slave1:9092");
		kafkaProp.put("key.deserializer", StringDeserializer.class.getCanonicalName());
		kafkaProp.put("value.deserializer", StringDeserializer.class.getCanonicalName());
		kafkaProp.put("group.id", "g1");
		kafkaProp.put("auto.offset.reset", "latest");
		kafkaProp.put("enable.auto.commit", "false");

		//主题
		List<String> topics = new ArrayList<String>() ;
		//主题
		topics.add("topic-app-startup") ;
//		topics.add("topic1") ;

		//创建kafka流
		JavaInputDStream<ConsumerRecord<Object,Object>> ds1 = KafkaUtils.createDirectStream(ssc,
				LocationStrategies.PreferBrokers(), ConsumerStrategies.Subscribe(topics,kafkaProp));
		//流变换
		JavaDStream<AppStartupLog> ds2 = ds1.map(new Function<ConsumerRecord<Object,Object>, AppStartupLog>() {
			public AppStartupLog call(ConsumerRecord<Object, Object> msg) throws Exception {
				String topic = msg.topic();
				String key = (String) msg.key();
				String value = (String) msg.value();
				AppStartupLog log = JSONObject.parseObject(value, AppStartupLog.class);
				return log ;
			}
		});
		//变换成key-value对
		JavaPairDStream<String,AppStartupLogAgg> ds3 = ds2.mapToPair(new PairFunction<AppStartupLog, String, AppStartupLogAgg>() {
			public Tuple2<String, AppStartupLogAgg> call(AppStartupLog log) throws Exception {
				String appid = log.getAppId() ;
				String devicecid = log.getDeviceId();
				String appversion = log.getAppVersion() ;
				String platform = log.getAppPlatform() ;

				String key = appid + "," + devicecid + "," + appversion + "," + platform ;
				AppStartupLogAgg logAgg = new AppStartupLogAgg();
				//初始化firatTime和lastTime
				logAgg.setFirstTime(log.getCreatedAtMs());
				logAgg.setLastTime(log.getCreatedAtMs());
				logAgg.setLog(log);

				return new Tuple2<String, AppStartupLogAgg>(key, logAgg);
			}
		});

		//聚合
		JavaPairDStream<String, AppStartupLogAgg> ds4 = ds3.reduceByKey(new Function2<AppStartupLogAgg, AppStartupLogAgg, AppStartupLogAgg>() {
			public AppStartupLogAgg call(AppStartupLogAgg v1, AppStartupLogAgg v2) throws Exception {
				long firstTime = Math.min(v1.getFirstTime(),v2.getFirstTime());
				long lastTime = Math.max(v1.getLastTime(),v2.getLastTime());
				AppStartupLogAgg agg = new AppStartupLogAgg();
				agg.setLog(v1.getLog());
				agg.setFirstTime(firstTime);
				agg.setLastTime(lastTime);
				return agg;
			}
		});

		//将同一app的聚合数据分到一组,统一入库
		JavaPairDStream<String, AppStartupLogAgg> ds5 = ds4.mapToPair(new PairFunction<Tuple2<String,AppStartupLogAgg>, String, AppStartupLogAgg>() {
			public Tuple2<String, AppStartupLogAgg> call(Tuple2<String, AppStartupLogAgg> t) throws Exception {
				String key = t._2().getLog().getAppId();
				return new Tuple2<String, AppStartupLogAgg>(key,t._2()) ;
			}
		});

		//分组
		JavaPairDStream<String,Iterable<AppStartupLogAgg>> ds6 = ds5.groupByKey(20);

		//循环聚合结果，插入phoenix库
		ds6.foreachRDD(new VoidFunction<JavaPairRDD<String, Iterable<AppStartupLogAgg>>>() {
			public void call(JavaPairRDD<String, Iterable<AppStartupLogAgg>> rdd) throws Exception {
				rdd.foreach(new VoidFunction<Tuple2<String, Iterable<AppStartupLogAgg>>>() {
					public void call(Tuple2<String, Iterable<AppStartupLogAgg>> tt) throws Exception {
						String appid = tt._1() ;
						Iterator<AppStartupLogAgg> it = tt._2().iterator();

						Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
						Connection conn = DriverManager.getConnection("jdbc:phoenix:master:2181");
						conn.setAutoCommit(false);
						//循环所有聚合数据
						while(it.hasNext()){
							AppStartupLogAgg agg = it.next();
							upsert2Phoenix(conn, appid, agg);
						}
						conn.commit();
					}
				});
			}
		});



		ds2.print();
		ssc.start();
		ssc.awaitTermination();
	}
	//输出数据
	public static void saveAgg(String key , AppStartupLogAgg agg){
		if(agg != null){
			System.out.println(key + " : " + agg.getFirstTime() + "," + agg.getLastTime());
		}
	}

	/**
	 * 数据入库
	 */
	public static void upsert2Phoenix(Connection conn , String table, AppStartupLogAgg agg){
		try {
			Statement st = conn.createStatement();
			String sql = "create table if not exists "
								 + table
								 + "(id varchar(50) primary key , "
								 + "deviceid varchar(20),"
								 + "appversion varchar(20),"
								 + "firsttime bigint,"
								 + "lasttime bigint)" ;
			//创建表
			st.execute(sql) ;
			//查看指定的记录是否存在
			AppStartupLog log = agg.getLog() ;
			String deviceid = log.getDeviceId() ;
			String appver = log.getAppVersion() ;
			String appPlat = log.getAppPlatform() ;

			String id = deviceid + "," + appver + "," + appPlat ;
			sql  = "select count(1) from " + table + " where id = '"+id+"'";

			ResultSet rs = st.executeQuery(sql);
			rs.next();
			long count = rs.getLong(1) ;
			//insert
			if(count == 0){
				sql = "upsert into " + table + "(id,deviceid,appversion,firsttime,lasttime) values(?,?,?,?,?)"  ;
				PreparedStatement ppst = conn.prepareStatement(sql);
				ppst.setString(1,id);
				ppst.setString(2,deviceid);
				ppst.setString(3,appver);
				ppst.setLong(4,agg.getFirstTime());
				ppst.setLong(5,agg.getLastTime());
				ppst.executeUpdate();
			}
			//update
			else{
				sql = "upsert into " + table + "(id,lasttime) values(?,?) ";
				PreparedStatement ppst = conn.prepareStatement(sql);
				ppst.setString(1, id);
				ppst.setLong(2, agg.getLastTime());
				ppst.executeUpdate();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
