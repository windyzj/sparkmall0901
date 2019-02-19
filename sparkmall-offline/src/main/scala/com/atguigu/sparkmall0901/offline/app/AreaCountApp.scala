package com.atguigu.sparkmall0901.offline.app

import java.util.Properties

import com.atguigu.sparkmall0901.common.utils.PropertiesUtil
import com.atguigu.sparkmall0901.offline.udf.CityRatioUDAF
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaCountApp {

  def main(args: Array[String]): Unit = {
       val sparkConf: SparkConf = new SparkConf().setAppName("area_count").setMaster("local[*]")
       val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
        val properties: Properties = PropertiesUtil.load("config.properties")
    sparkSession.udf.register("city_ratio",new CityRatioUDAF)
    sparkSession.sql("use sparkmall0901")
//    1 、 关联城市表和用户行为表
    sparkSession.sql("select ci.area,ci.city_name,click_product_id  from user_visit_action uv inner join city_info ci where  uv.city_id=ci.city_id and click_product_id >0").createOrReplaceTempView("v_action_city")
//    2 、 按 地区 商品 进行分组 count
    sparkSession.sql("select area,click_product_id ,count(*) clickcount, city_ratio(city_name) city_remark from v_action_city group by  area,click_product_id ").createOrReplaceTempView("v_area_product_clickcount")
// 3 把商品在地区中的排名取出来
   sparkSession.sql(" select     area,click_product_id ,clickcount,city_remark      from    (select  v.*, rank()over(partition by area order by clickcount desc ) rk  from v_area_product_clickcount v) clickrk where rk<=3").createOrReplaceTempView("v_area_product_clickcount_top3")
//      3      截取前三的商品   关联 商品表  取到商品名称
  sparkSession.sql(" select area,p.product_name,clickcount,city_remark from v_area_product_clickcount_top3 t3 ,product_info p where t3.click_product_id=p.product_id ").write.format("jdbc")
    .option("url",properties.getProperty("jdbc.url"))
      .option("user",properties.getProperty("jdbc.user"))
      .option("password",properties.getProperty("jdbc.password"))
      .option("dbtable","area_count_info").mode(SaveMode.Append).save()


  }

}
