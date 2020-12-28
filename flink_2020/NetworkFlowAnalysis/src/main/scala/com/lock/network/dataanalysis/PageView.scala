package com.lock.network.dataanalysis

import com.lock.network.entry.{PvCount, UserBehavior}
import com.lock.network.function.{PrePVAcc, PvWindowFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * 直接算 和 防止数据倾斜，把数据进行分区再聚合
  *
  * author  Lock.xia
  * Date 2020-12-25
  */
object PageView {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    // 对数据进行转化
    val dataStream: DataStream[UserBehavior] = inputStream
      .map((k: String) => {
        val dataArr: Array[String] = k.split(",")
        UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
      })
      .assignAscendingTimestamps((_: UserBehavior).timestamp * 1000L)

    // 对数据进行PV统计
    val keyedStreamPV: KeyedStream[(String, Long), String] = dataStream
      .filter((_: UserBehavior).behavior == "pv")
      .map((_: UserBehavior) => ("pv", 1L)) // 隐患：在接下来的keyBy操作中，会将数据放到一个分区,数据倾斜，OOM
      .keyBy((_: (String, Long))._1)


    val pvWindowStream: DataStream[PvCount] = keyedStreamPV.timeWindow(Time.hours(1)) // 开一个小时的滚动窗口
      .aggregate(new PrePVAcc(), new PvWindowFunction())
      .keyBy((_: PvCount).WindowEnd)
      .sum("count")

    keyedStreamPV.print("keyPV")
    pvWindowStream.print("pvResult")

    env.execute("page pv ")

  }
}
