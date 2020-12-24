package com.lock.network.dataanalysis

import java.text.SimpleDateFormat

import com.lock.network.entry.{ApacheLogEvent, PageViewCount}
import com.lock.network.function.{PageProcess, UrlPreCount, UrlWindowCount}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * 每隔五秒，输出最近10min内访问量排名前N的URL
  *
  * author  Lock.xia
  * Date 2020-12-24
  */
object NetworkFlowPageTop {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 获取数据   log ==> 83.149.9.216 - - 17/05/2015:10:05:56 +0000 GET /favicon.ico
    val sourceDataStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")

    val resultStream: DataStream[String] = sourceDataStream
      .map((k: String) => {
        val dataArr: Array[String] = k.split(" ")
        //        val timeStamp: Long = new SimpleDateFormat("dd/MM/YY:HH:mm:ss").parse(dataArr(3)).getTime
        val timeStamp: Long = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(dataArr(3)).getTime
        ApacheLogEvent(dataArr(0), dataArr(2), timeStamp, dataArr(5), dataArr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(6 * 1000)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })
      .keyBy((_: ApacheLogEvent).url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new UrlPreCount(), new UrlWindowCount())
      .keyBy((_: PageViewCount).windowEnd)
      .process(new PageProcess(5))

    resultStream.print("result page count")

    env.execute("url hot statistics ")
  }
}