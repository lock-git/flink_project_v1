package com.lock.analysis

import com.lock.entry.{OrderEvent, OrderResult}
import com.lock.function.OrderPayProcessFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  *  processFunction 实现订单超时项目
  *
  * author  Lock.xia
  * Date 2021-01-12
  */
object OrderPayAnalysisNoCep {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\OrderLog.csv")

    val dataStream: DataStream[OrderEvent] = inputStream.map((k: String) => {
      val dataArr: Array[String] = k.split(",")
      OrderEvent(dataArr(0).toLong, dataArr(1), dataArr(2), dataArr(3).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(1)) {
      override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
    })

    // 定义一个侧输出流标签，将超时和异常订单输出到侧输出流
    val orderOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("order_timeout")

    val resultStream: DataStream[OrderResult] = dataStream
      .keyBy((_: OrderEvent).orderId)
      .process(new OrderPayProcessFunction())

    resultStream.print("success")
    resultStream.getSideOutput(orderOutputTag).print("timeout")

    env.execute("order pay timeout process")

  }

}
