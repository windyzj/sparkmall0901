package com.atguigu.sparkmall0901.realtime.handler

import java.util
import java.util.Properties

import com.atguigu.sparkmall0901.common.utils.PropertiesUtil
import com.atguigu.sparkmall0901.realtime.bean.AdsLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListHandler {

  def handle(adsLogDstream: DStream[AdsLog]): Unit ={
    //每日每用户每广告的点击次数
    val clickcountPerAdsPerDayDStream: DStream[(String, Long)] = adsLogDstream.map { adsLog => (adsLog.getDate() + "_" + adsLog.userId + "_" + adsLog.adsId, 1L) }.reduceByKey(_ + _)
    clickcountPerAdsPerDayDStream.foreachRDD(rdd=> {

      val prop: Properties = PropertiesUtil.load("config.properties")

      rdd.foreachPartition { adsItr =>
        //建立redis连接
        val jedis = new Jedis(prop.getProperty("redis.host"),prop.getProperty("redis.port").toInt) //driver
        //redis结构  hash  key: day  field: user_ads  value:count
        adsItr.foreach{  case (logkey, count) =>
          val day_user_ads: Array[String] = logkey.split("_")
          val day: String = day_user_ads(0)
          val user: String = day_user_ads(1)
          val ads: String = day_user_ads(2)
          val key="user_ads_click:"+day

          jedis.hincrBy(key,user+"_"+ads ,count )  //hincrby 累加
          //判断 是否达到了 100
          val curCount: String = jedis.hget(key,user+"_"+ads)
          if(curCount.toLong>=100){
            //黑名单key的类型：Set
            jedis.sadd("blacklist",user)
          }

        }
        jedis.close()

      }

    }
    )

  }

  def check(sparkContext: SparkContext, adsLogDstream: DStream[AdsLog]): DStream[AdsLog] ={

    //利用filter过滤   过滤依据是 blacklist

///      此部分的driver代码只会在启动时执行一次，会造成blacklist无法实时更新
//    val prop: Properties = PropertiesUtil.load("config.properties")
//    val jedis = new Jedis(prop.getProperty("redis.host"),prop.getProperty("redis.port").toInt)
//    val blacklistSet: util.Set[String] = jedis.smembers("blacklist")  //driver
//    val blacklistBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blacklistSet)
//    val filteredAdsLogDstream: DStream[AdsLog] = adsLogDstream.filter { adslog =>
//      !blacklistBC.value.contains(adslog)   //executor
//    }


    val filteredAdsLogDstream: DStream[AdsLog] = adsLogDstream.transform{rdd=>
     // driver 每个时间间隔执行一次
      val prop: Properties = PropertiesUtil.load("config.properties")
      val jedis = new Jedis(prop.getProperty("redis.host"),prop.getProperty("redis.port").toInt)
      val blacklistSet: util.Set[String] = jedis.smembers("blacklist")  //driver
      val blacklistBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blacklistSet)
      rdd.filter{ adslog =>
        println(blacklistBC.value)
        !blacklistBC.value.contains(adslog.userId)   //executor
      }


    }


    filteredAdsLogDstream
  }

}
