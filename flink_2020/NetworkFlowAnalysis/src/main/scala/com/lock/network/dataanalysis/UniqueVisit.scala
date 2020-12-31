package com.lock.network.dataanalysis

import com.lock.network.entry.{UserBehavior, UvCount}
import com.lock.network.function.UvAllWindowFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * 问题点：没有考虑数据倾斜；全量处理，没有考虑增量聚合方式；没有考虑数据量过大，set的问题
  *
  *
  *
  * author  Lock.xia
  * Date 2020-12-29
  */
object UniqueVisit {
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
      .timeWindowAll(Time.hours(1)) // 滚动窗口1h ，全窗口  hash碰撞 10倍的余量，
      .apply(new UvAllWindowFunction())

    resultStream.print("result")

    env.execute("uv count")
  }
}
