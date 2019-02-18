package com.atguigu.sparkmall0901.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]]{

   var categoryMap:mutable.HashMap[String,Long]=new mutable.HashMap[String,Long]()

  //判空
  override def isZero: Boolean = categoryMap.isEmpty

  //复制
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
      new CategoryAccumulator()
  }

  //重置
  override def reset(): Unit = {
    categoryMap.clear()
  }

  //累加
  override def add(key: String): Unit = {
       categoryMap(key)= categoryMap.getOrElse(key,0L)+1L
  }

  //合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val otherMap: mutable.HashMap[String, Long] = other.value
    //两个map 根据key值进行 合并
    categoryMap = categoryMap.foldLeft(otherMap) { case (otherMap, (key, count)) =>
      otherMap(key) = otherMap.getOrElse(key, 0L) + count //如果key相同进行累加
      otherMap
    }

  }

  //返回值
  override def value: mutable.HashMap[String, Long] = {
    categoryMap
  }
}
