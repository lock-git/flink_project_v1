package com.lock.login.detect

import com.lock.login.entry.{LoginEvent, LoginFailWarning}
import com.lock.login.function.LoginWarn
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * 需求：用户连续2s内登陆失败N次，触发报警
  *
  * 缺点： // 存在乱序数据【本应该在2s内出现的成功登入日志，延迟过来，没有告警】
  *       // 2s之内包含多次失败，一次成功 【在业务理解范围内也应该告警】
  *
  * author  Lock.xia
  * Date 2021-01-05
  */
object LoginFail {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[LoginEvent] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\LoginLog.csv")
      .map((k: String) => {
        val dataArr: Array[String] = k.split(",")
        LoginEvent(dataArr(0).toLong, dataArr(1).toString, dataArr(2).toString, dataArr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(1)) {
        override def extractTimestamp(element: LoginEvent): Long = {
          element.eventTime * 1000L
        }
      })

    val resultStream: DataStream[LoginFailWarning] = dataStream
      .keyBy((_: LoginEvent).userId)
      .process(new LoginWarn(3))

    resultStream.print("result")

    env.execute("login fail N warn")
  }

}

