package com.lock.network.dataanalysis

import java.text.SimpleDateFormat

import com.lock.network.entry.{ApacheLogEvent, PageViewCount}
import com.lock.network.function.{PageCountProcessFunction, PageWindowFunction, PrePageCount}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * 对于延迟数据，有三种方式解决【watermark allowed-lateness sideOutput】
  *
  * author  Lock.xia
  * Date 2020-12-24
  */
object NetworkFlowPageTop_P {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")

    val inputStream: DataStream[String] = env.socketTextStream("localhost",7777) // [ nc -l -p 7777 ]

    // 侧输出流的标签
    val lateDataTag = new OutputTag[ApacheLogEvent]("late_data")

    // 83.149.9.216 - - 17/05/2015:10:05:56 +0000 GET /favicon.ico
    val dataStream: DataStream[ApacheLogEvent] = inputStream
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

    val aggStream: DataStream[PageViewCount] = dataStream
      .keyBy((_: ApacheLogEvent).url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1)) //[ 定义延迟时间 ] 等待迟到数据 1 min ，将之前窗口的数据和延迟的数据一起聚合，再更新迭代数据，等待新的watermark触发输出数据，最后等延迟时间到了再关闭窗口，清空窗口数据
      .sideOutputLateData(lateDataTag) // 侧输出流
      .aggregate(new PrePageCount(), new PageWindowFunction())

    // 获取侧输出流的数据 ==》 可以跟之前的流做一个合流操作，得到最终的数据
    val lateStream: DataStream[ApacheLogEvent] = aggStream.getSideOutput(lateDataTag)

    val resultStream: DataStream[String] = aggStream
      .keyBy((_: PageViewCount).windowEnd)
      .process(new PageCountProcessFunction(5))

    dataStream.print("input")
    aggStream.print("agg")
    lateStream.print("late")
    resultStream.print("topResult")

    env.execute("page top n")

  }

}
