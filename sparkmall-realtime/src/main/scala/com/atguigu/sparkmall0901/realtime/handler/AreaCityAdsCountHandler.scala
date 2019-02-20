package com.atguigu.sparkmall0901.realtime.handler

import java.util.Properties

import com.atguigu.sparkmall0901.common.utils.PropertiesUtil
import com.atguigu.sparkmall0901.realtime.bean.AdsLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object AreaCityAdsCountHandler {

//  1 、 整理dstream => kv 结构(date:area:city:ads,1L)
  //  2、利用updateStateBykey进行累加
//  3、把结果保存到redis,    hash     hset
//
//  DStream[(k,v)] .updateStateByKey(countSeq:Seq[Long], total:Option[Long] =>



  def handle(adsLogDstream: DStream[AdsLog]): Unit = {


    //  1 、 整理dstream => kv 结构(date:area:city:ads,1L)
    val adsClickDstream: DStream[(String, Long)] = adsLogDstream.map { adslog =>
      val key: String = adslog.getDate() + ":" + adslog.area + ":" + adslog.city + ":" + adslog.adsId
      (key, 1L)
    }
    //  2、利用updateStateBykey进行累加
    val adsClickCountDstream: DStream[(String, Long)] = adsClickDstream.updateStateByKey { (countSeq: Seq[Long], total: Option[Long]) =>
      //把countSeq汇总   累加在total里
      println(countSeq.mkString(","))
      val countSum: Long = countSeq.sum
      val curTotal = total.getOrElse(0L) + countSum
      Some(curTotal)
    }
    //  3、把结果保存到redis,    hash     hset
    adsClickCountDstream.foreachRDD { rdd =>
      val prop: Properties = PropertiesUtil.load("config.properties")
      rdd.foreachPartition { adsClickCountItr =>
        //建立redis连接
        val jedis = new Jedis(prop.getProperty("redis.host"), prop.getProperty("redis.port").toInt) //driver
        adsClickCountItr.foreach { case (key, count) =>
          jedis.hset("date:area:city:ads", key, count.toString)
        }
        jedis.close()
      }

    }
  }
}
