package com.atguigu.sparkmall0901.offline.handler

import com.atguigu.sparkmall0901.common.bean.UserVisitAction
import com.atguigu.sparkmall0901.common.utils.JdbcUtil
import com.atguigu.sparkmall0901.offline.acc.CategoryAccumulator
import com.atguigu.sparkmall0901.offline.bean.CategoryCount
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

//需求一
object CategoryCountHandler {

  def handle(sparkSession: SparkSession,userVisitActionRDD:RDD[UserVisitAction] ,taskId:String): List[CategoryCount] ={
    //    2 定义累加器  注册累加器
    val accumulator = new CategoryAccumulator
    sparkSession.sparkContext.register(accumulator)
    //    3 遍历rdd 利用累加器进行 累加操作
    userVisitActionRDD.foreach { userVisitAction =>
      if (userVisitAction.click_category_id != -1L) {
        val key: String = userVisitAction.click_category_id + "_click"
        accumulator.add(key)
      } else if (userVisitAction.order_category_ids != null && userVisitAction.order_category_ids.length > 0) {
        val orderCids: Array[String] = userVisitAction.order_category_ids.split(",")
        for (cid <- orderCids) {
          val key: String = cid + "_order"
          accumulator.add(key)
        }

      } else if (userVisitAction.pay_category_ids != null && userVisitAction.pay_category_ids.length > 0) {
        val payCids: Array[String] = userVisitAction.pay_category_ids.split(",")
        for (cid <- payCids) {
          val key: String = cid + "_pay"
          accumulator.add(key)
        }

      }
    }

    //
    //    4 得到累加器的结果
    val categoryMap: mutable.HashMap[String, Long] = accumulator.value
    println(s"categoryMap = ${categoryMap.mkString("\n")}")
    //4.1 把结果转化成 List[CategoryCount]  //模式匹配能够帮你把 结合里面的元素直接出入到某个变量中
    val categoryGroupCidMap: Map[String, mutable.HashMap[String, Long]] = categoryMap.groupBy({case (key,count)=> key.split("_")(0)})
    println(s"categoryGroupCidMap = ${categoryGroupCidMap.mkString("\n")}")
    val categoryCountList:List[CategoryCount] = categoryGroupCidMap.map { case (cid, actionMap) =>

      CategoryCount("", cid, actionMap.getOrElse(cid + "_click", 0L), actionMap.getOrElse(cid + "_order", 0L), actionMap.getOrElse(cid + "_pay", 0L))
    }.toList


    //
    //    5 把结果进行排序 、截取
    val sortedCategoryCountList: List[CategoryCount] = categoryCountList.sortWith { (categoryCount1, categoryCount2) =>
      //以 点击位置  点击量相同的 比较下单 ，下单相同 比较支付
      //升序还是降序  前小后大true == 升序    前大后小true == 降序
      if (categoryCount1.clickCount > categoryCount2.clickCount) {
        true
      } else if (categoryCount1.clickCount == categoryCount2.clickCount) {
        if (categoryCount1.orderCount > categoryCount2.orderCount) {
          true
        } else {
          false
        }
      } else {
        false
      }
    }.take(10)
    //    6 前十保存到mysql
    println(s"sortedCategoryCountList = ${sortedCategoryCountList.mkString("\n")}")

    val resultList: List[Array[Any]] = sortedCategoryCountList.map{categoryCount=> Array(taskId,categoryCount.categoryId,categoryCount.clickCount,categoryCount.orderCount,categoryCount.payCount)   }

    JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)",resultList)

    sortedCategoryCountList
  }

}
