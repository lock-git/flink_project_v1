package com.lock.network.dataanalysis

import java.text.SimpleDateFormat

import com.lock.network.entry.{ApacheLogEvent, PageViewCount}
import com.lock.network.function.{PageCountProcessFunction, PageWindowFunction, PrePageCount}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * author  Lock.xia
  * Date 2020-12-24
  */
object NetworkFlowPageTop_P {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    val lateDataTag = new OutputTag[ApacheLogEvent]("late_data")

    // 83.149.9.216 - - 17/05/2015:10:05:56 +0000 GET /favicon.ico
    val aggStream: DataStream[PageViewCount] = inputStream
      .map((k: String) => {
        val dataArr: Array[String] = k.split(" ")
        val time: Long = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(dataArr(3)).getTime
        ApacheLogEvent(dataArr(0), dataArr(2), time, dataArr(5), dataArr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) { // 大部分延迟在一秒之内
        override def extractTimestamp(element: ApacheLogEvent): Long = {
          element.eventTime
        }
      })
      .keyBy((_: ApacheLogEvent).url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1)) // 等待迟到数据 1min ，将之前窗口的数据迭代
      .sideOutputLateData(lateDataTag) // 侧输出流
      .aggregate(new PrePageCount(), new PageWindowFunction())


    val resultStream: DataStream[String] = aggStream
      .keyBy((_: PageViewCount).windowEnd)
      .process(new PageCountProcessFunction(5))

    // 迟到数据 单独操作，最后再合流
    val lateStream: DataStream[ApacheLogEvent] = aggStream.getSideOutput(lateDataTag)

    resultStream.print("topResult")

    env.execute("page top n")

  }

}
