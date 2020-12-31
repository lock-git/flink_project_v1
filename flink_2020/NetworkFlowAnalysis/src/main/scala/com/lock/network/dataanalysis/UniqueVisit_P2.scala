package com.lock.network.dataanalysis

import com.lock.network.entry.{UserBehavior, UvCount}
import com.lock.network.function.{UvBloomFilter, UvMapper, UvTrigger}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * 使用布隆过滤器原理来进行统计海量用户UV
  *
  * author  Lock.xia
  * Date 2020-12-30
  */
object UniqueVisit_P2 {
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
      .map(new UvMapper())
      .keyBy((_: (String, Long))._1)
      .timeWindow(Time.hours(1))
      .trigger(new UvTrigger()) // 触发器
      .process(new UvBloomFilter())

    resultStream.print("result")


    env.execute("uv pro bloom")


  }
}
