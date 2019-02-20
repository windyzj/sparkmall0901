package com.atguigu.sparkmall0901.realtime.handler

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall0901.common.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import org.json4s.JsonDSL._
import redis.clients.jedis.Jedis

object AreaTop3AdsHandler {

//  day_area_City_adskey:count
//  1、 去掉城市 在进行聚合  得到 每天每地区每广告的计数
//  .map{ day_area_City_adskey:count  ->day_area_adskey:count }.reducebykey
//
//  2
//  想办法把 纯kv结构 变成层级结构
//  RDD[day_area_adskey:count] =>RDD[(day,(area,(adskey,count)))]
//    .groupbykey
//  =>
//  RDD[(day,Iterable(area,(adskey,count)))]
//  .map{ areaItr
//    areaItr.groupby(指定用area进行分组)
//    =>map[area,Iterable(area,(adskey,count)].map{}
//    =>map[area,map[adskey,count]}
//}
//=>day ,map[area,map[adskey,count]}
//目标 ：daykey:[map[area: adsTop3map[adsid:count]  ]]
//
//
//daykey:[map[area: json  ]]
  def handle(areaCityAdsCountDstream: DStream[(String, Long)]): Unit ={
  //  day_area_City_adskey:count
  //  1、 去掉城市 在进行聚合  得到 每天每地区每广告的计数
  //  .map{ day_area_City_adskey:count  ->day_area_adskey:count }.reducebykey
  val areaAdsCountDstream: DStream[(String, Long)] = areaCityAdsCountDstream.map { case (day_area_city_adskey, count) =>
    val keyArr: Array[String] = day_area_city_adskey.split(":")
    val daykey: String = keyArr(0)
    val area: String = keyArr(1)
    val city: String = keyArr(2)
    val adsId: String = keyArr(3)
    (daykey + ":" + area + ":" + adsId, count)
  }.reduceByKey(_ + _)

  //  2
  //  想办法把 纯kv结构 变成层级结构
  //  RDD[day_area_adskey:count] =>RDD[(day,(area,(adskey,count)))]
  //    .groupbykey
  //  =>
  //  RDD[(day,Iterable(area,(adskey,count)))]
  val areaAdsCountGroupbyDayDStream: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsCountDstream.map { case (day_area_adskey, count) =>
    val keyArr: Array[String] = day_area_adskey.split(":")
    val daykey: String = keyArr(0)
    val area: String = keyArr(1)
    val adsId: String = keyArr(2)
    (daykey, (area, (adsId, count)))
  }.groupByKey()
//3     准备小组赛 ，按地区area进行分组
  val areaAdsTop3JsonCountGroupbyDayDStream: DStream[(String, Map[String, String])] = areaAdsCountGroupbyDayDStream.map { case (daykey, areaItr) =>
    val adsCountGroupbyAreaMap: Map[String, Iterable[(String, (String, Long))]] = areaItr.groupBy { case (area, (adsid, count)) => area }
    val adsTop3CountGroupbyAreaMap: Map[String, String] = adsCountGroupbyAreaMap.map { case (area, adsItr) =>
      //  iterable里面的area 冗余 给我去掉  ， 排序  ， 截取前三 , 变json
      val top3AdsList: List[(String, Long)] = adsItr.map { case (area, (adsId, count)) => (adsId, count) }.toList.sortWith(_._2 > _._2).take(3)
      val top3AdsJsonString: String = JsonMethods.compact(JsonMethods.render(top3AdsList))
      (area, top3AdsJsonString)
    }
    (daykey, adsTop3CountGroupbyAreaMap)
  }  // 每个日期对一个地区的Map  地区的map里 地区名+ 前三名广告点击量的json
  areaAdsTop3JsonCountGroupbyDayDStream
  //4    保存到redis中
  areaAdsTop3JsonCountGroupbyDayDStream.foreachRDD{rdd=>
    //driver
    rdd.foreachPartition{ dayItr=>
      //executor
      val jedisClient: Jedis = RedisUtil.getJedisClient
      dayItr.foreach{case (daykey ,areaMap)=>
        import  collection.JavaConversions._
        jedisClient.hmset("area_top3_ads:"+daykey,areaMap);
      }
      jedisClient.close()

    }
  }
  }

}
