package com.lock.network.dataanalysis

import com.lock.network.entry.{AdClickLog, BlackListWarning, CountByProvince}
import com.lock.network.function.{AdBlackListFilter, AdClickWindowFunction, AdProClickFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment,_}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Random

/**
  *
  * 对于在一定时间内，点击次数大于N值的用户，加入黑名单，且不再累加该用户的点击次数
  *
  * author  Lock.xia
  * Date 2021-01-04
  */
object AdClickBlackList {


  // 定义侧输出流标签
  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(4)



    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\AdClickLog.csv")

    val dataStream: DataStream[AdClickLog] = inputStream.map((k: String) => {
      val dataArr: Array[String] = k.split(",")
      AdClickLog(dataArr(0).toLong, dataArr(1).toLong, dataArr(2), dataArr(3), dataArr(4).toLong * 1000L)
    }).assignAscendingTimestamps((_: AdClickLog).timestamp)

    // 对于当日点击广告次数超过100的用户，拉入黑名单,通过侧输出流输出
    val blackListFilterStream: DataStream[AdClickLog] = dataStream
      .keyBy((k: AdClickLog) => (k.userId, k.adId))
      .process(new AdBlackListFilter(100))

    val aggStream: DataStream[CountByProvince] = blackListFilterStream
      .map((k: AdClickLog) => (k.province + "-" + Random.nextInt(10).toString, 1L))
      .keyBy((_: (String, Long))._1)
      .timeWindow(Time. hours(1), Time.minutes(5))
      .aggregate(new AdProClickFunction(), new AdClickWindowFunction())

    val resultStream: DataStream[((String, String), Long)] = aggStream
      .map((m: CountByProvince) => ((m.windowEnd, m.province.split("-")(0)), m.count))
      .keyBy((_: ((String, String), Long))._1)
      .sum(1)

    val outputBlackStream: DataStream[BlackListWarning] = blackListFilterStream.getSideOutput[BlackListWarning](blackListOutputTag)

    outputBlackStream.print("black")

    resultStream.print("result")

    env.execute("black filter")
  }

}
