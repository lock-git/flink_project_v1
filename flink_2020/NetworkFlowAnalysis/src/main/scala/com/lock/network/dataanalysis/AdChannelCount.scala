package com.lock.network.dataanalysis

import com.lock.network.entry.{MarketingCountView, MarketingUserBehavior}
import com.lock.network.function.AdProcessWindowFunction
import com.lock.network.util.AdSource
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * 根据行为，渠道分组，统计每小时广告的点击量
  * 滑动窗口：1小时，1s[测试]滑动一次
  * 过滤：behavior == "UNINSTALL"
  *
  * author  Lock.xia
  * Date 2021-01-04
  */
object AdChannelCount {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[MarketingUserBehavior] = env.addSource(new AdSource())

    val adResultStream: DataStream[MarketingCountView] = inputStream
      .assignAscendingTimestamps((_: MarketingUserBehavior).timestamp)
      .filter((_: MarketingUserBehavior).behavior != "UNINSTALL")
      .keyBy((k: MarketingUserBehavior) => (k.behavior, k.channel))
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new AdProcessWindowFunction())

//      .aggregate(new AdProAggFunction(), new AdChannelWindowFunction())

    adResultStream.print("result")

    env.execute("ad channel count")

  }

}
