package com.lock.network.dataanalysis

import com.lock.network.entry.{PvCount, UserBehavior}
import com.lock.network.function._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * author  Lock.xia
  * Date 2020-12-28
  */
object PageView_Pro2 {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream.map((k: String) => {
      val dataArr: Array[String] = k.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toLong, dataArr(3), dataArr(4).toLong)
    })
      .assignAscendingTimestamps((_: UserBehavior).timestamp * 1000L)


    val aggStream: DataStream[PvCount] = dataStream
      .filter((_: UserBehavior).behavior == "pv")
      .map(new PvMap())
      .keyBy((_: (String, Long))._1)
      .timeWindow(Time.hours(1))
      .aggregate(new PrePVAcc(), new PvWindowFunction())

    val resultStream: DataStream[PvCount] = aggStream
      .keyBy((_: PvCount).WindowEnd)
      .process(new PagePvProcess())

    aggStream.print("agg")
    resultStream.print("result")

    env.execute("page pv")

  }
}
