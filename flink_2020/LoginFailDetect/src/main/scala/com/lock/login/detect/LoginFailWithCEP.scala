package com.lock.login.detect

import com.lock.login.entry.LoginEvent
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * author  Lock.xia
  * Date 2021-01-06
  */
object LoginFailWithCEP {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[LoginEvent] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\LoginLog.csv")
      .map((k: String) => {
        val dataArr: Array[String] = k.split(",")
        LoginEvent(dataArr(0).toLong, dataArr(1).toString, dataArr(2).toString, dataArr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = {
          element.eventTime * 1000L
        }
      })


    // 定义 CEP 的 匹配模式
    val loginFailPattern:Pattern[LoginEvent,LoginEvent] = Pattern
      .begin[LoginEvent]("firstFail").where((_: LoginEvent).eventType == "fail") // 第一次登入失败
      .next("secondFail").where((_: LoginEvent).eventType == "fail") // 第二次登入失败
      .within(Time.seconds(2)) // 在2s内检测匹配





  }

}
