package com.lock.network.dataanalysis

import com.lock.network.entry.{UserBehavior, UvCount}
import com.lock.network.function.{UvProFunction, UvWindowFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * 优点：采用增量聚合的方式
  * ？？？？？BUG===>  java.lang.NumberFormatException: Not a version: 9
  *
  * author  Lock.xia
  * Date 2020-12-29
  */
object UniqueVisit_P {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream
      .map((k: String) => {
        val dataArr: Array[String] = k.split(",")
        UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toLong, dataArr(3), dataArr(4).toLong)
      })
      .assignAscendingTimestamps((_: UserBehavior).timestamp * 1000L)

    val resultStream: DataStream[UvCount] = dataStream
      .filter((_: UserBehavior).behavior == "pv")
      .timeWindowAll(Time.hours(1)) // 滚动窗口1h ，全窗口
      .aggregate(new UvProFunction(), new UvWindowFunction())

    resultStream.print("result")

    env.execute("uv count pro")
  }
}
