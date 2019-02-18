package com.atguigu.sparkmall0901.offline.handler

import com.atguigu.sparkmall0901.common.bean.UserVisitAction
import com.atguigu.sparkmall0901.offline.bean.CategoryCount
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategoryTopSessionHandler {


  def handle( sparkSession: SparkSession, userVisitActionRDD:RDD[ UserVisitAction],taskId:String,top10CategoryList:List[CategoryCount]): Unit ={

    val cidTop10: List[String] = top10CategoryList.map(_.categoryId)
    val cidTop10BC: Broadcast[List[String]] = sparkSession.sparkContext.broadcast(cidTop10)
//    1   RDD[UserVisitAction]  过滤 ,保留top10品类的点击
    val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter { userVisitAction =>
      cidTop10BC.value.contains(userVisitAction.click_category_id)
    }
    filteredUserVisitActionRDD


//    2   RDD[UserVisitAction] 统计次数    得到每个session 点击 top10品类的次数
//      rdd->k-v结构  .map(action.category_click_id+"_"+action.sessionId,1L)
//    ->.reducebykey(_+_)
//    ->RDD[action.category_click_id+"_"+action.sessionID,count]
    val clickCountGroupByCidSessionRDD: RDD[(String, Long)] = filteredUserVisitActionRDD.map(action=>(action.click_category_id+"_"+action.session_id,1L)).reduceByKey(_+_)

    //    3  分组 准备做组内排序  以品类id  分组
    val sessionCountGroupbyCidRdd: RDD[(String, Iterable[(String, Long)])] = clickCountGroupByCidSessionRDD.map { case (cidSession, count) =>
      val cidSessionArr: Array[String] = cidSession.split("_")
      val cid: String = cidSessionArr(0)
      val sessionId: String = cidSessionArr(1)
      (cid, (sessionId, count))
    }.groupByKey()


    sessionCountGroupbyCidRdd
//  4 小组赛  保留每组的前十名
    sessionCountGroupbyCidRdd.map{case(cid,sessionItr)=>
      sessionItr.toList.sortWith{(sessionCount1,sessionCount2)=>
        sessionCount1._2>sessionCount2._2
      }.take(10)
    }
    //  RDD[action.category_click_id+"_"+action.sessionID,count]  -> map
//
//    ->RDD[action.category_click_id,(action.sessionID,count)] ->groupbykey
//      ->RDD[action.category_click_id,Iterable[(action.sessionID,count)]->
//      rdd.map{
//        itr.sortwith().take(10)
//      }
//
//    RDD[Array[Any]]  .collect  ->save mysql
  }
}
