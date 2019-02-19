package com.atguigu.sparkmall0901.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class AdsLog (ts:Long,area:String ,city:String,userId:String ,adsId:String){
   val dateFormat=  new SimpleDateFormat("yyyy-MM-dd")
  def  getDate( ): String ={
    dateFormat.format(new Date(ts))
  }

}
