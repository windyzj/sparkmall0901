package com.atguigu.sparkmall0901.mock.util

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random

object RandomDate {

  def apply(startDate:Date,endDate:Date,step:Int): RandomDate ={
    val randomDate = new RandomDate()
    val avgStepTime = (endDate.getTime- startDate.getTime)/step
    randomDate.maxTimeStep=avgStepTime*2
    randomDate.lastDateTime=startDate.getTime
    randomDate
  }

  def main(args: Array[String]): Unit = {
      val randomDate = RandomDate(new SimpleDateFormat("yyyy-MM-dd").parse("2019-01-01"),new SimpleDateFormat("yyyy-MM-dd").parse("2019-01-03"),50)
     for(i<-1 to 50 ){
       println(s"randomDate.getRandomDate(); = ${randomDate.getRandomDate();}")
     }   
    
  }

  class RandomDate{
    var lastDateTime =0L
    var maxTimeStep=0L

    def  getRandomDate()={
      val timeStep = new Random().nextInt(maxTimeStep.toInt)
      lastDateTime = lastDateTime+timeStep

      new Date( lastDateTime)
    }
  }
}
