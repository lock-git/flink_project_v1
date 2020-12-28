package com.lock.network.dataanalysis

import com.lock.network.entry.{PvCount, UserBehavior}
import com.lock.network.function.{PrePVAcc, PvMap, PvWindowFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * 统计PV时，避免数据倾斜
  *
  * author  Lock.xia
  * Date 2020-12-28
  */
object PageView_Pro {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
//    val inputStream: DataStream[String] = env.socketTextStream("localhost",7777)

    val keyedPvStream: KeyedStream[(String, Long), String] = inputStream
      .map((k: String) => {
        val dataArr: Array[String] = k.split(",")
        UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toLong, dataArr(3), dataArr(4).toLong)
      })
      .assignAscendingTimestamps((_: UserBehavior).timestamp * 1000L)
      .filter((_: UserBehavior).behavior == "pv")
      .map(new PvMap())
      .keyBy((_: (String, Long))._1)

    val resultStream1: DataStream[PvCount] = keyedPvStream
      .timeWindow(Time.hours(1))
      .aggregate(new PrePVAcc(), new PvWindowFunction())
    val resultStream: DataStream[PvCount] = resultStream1
      .keyBy((_: PvCount).WindowEnd)
      .sum("count")

//    inputStream.print("input")
//    keyedPvStream.print("keyed")
//    resultStream1.print("result1")
    resultStream.print("result")


    env.execute("pv pro")
  }
}
