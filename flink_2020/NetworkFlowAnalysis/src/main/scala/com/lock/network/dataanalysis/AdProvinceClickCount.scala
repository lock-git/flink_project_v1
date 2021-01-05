package com.lock.network.dataanalysis

import com.lock.network.entry.{AdClickLog, CountByProvince}
import com.lock.network.function.{AdClickWindowFunction, AdProClickFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Random

/**
  *
  * 页面广告按照省份划分的点击量的统计
  * time window (1h,5s)
  *
  * author  Lock.xia
  * Date 2021-01-04
  */
object AdProvinceClickCount {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\AdClickLog.csv")


    val dataStream: DataStream[AdClickLog] = inputStream.map((k: String) => {
      val dataArr: Array[String] = k.split(",")
      AdClickLog(dataArr(0).toLong, dataArr(1).toLong, dataArr(2), dataArr(3), dataArr(4).toLong * 1000L)
    }).assignAscendingTimestamps((_: AdClickLog).timestamp)

    val aggStream: DataStream[CountByProvince] = dataStream
      .map((k: AdClickLog) => (k.province + "-" + Random.nextInt(10).toString, 1L))
      .keyBy((_: (String, Long))._1)
      .timeWindow(Time.minutes(30), Time.seconds(60))
      .aggregate(new AdProClickFunction(), new AdClickWindowFunction())

    val resultStream: DataStream[((String, String), Long)] = aggStream
      .map((m: CountByProvince) => ((m.windowEnd, m.province.split("-")(0)), m.count))
      .keyBy((_: ((String, String), Long))._1)
      .sum(1)

    //    dataStream.print("input")
    aggStream.print("agg")
    resultStream.print("result")

    env.execute("ad province click count")

  }
}


