package com.lock.analysis

import com.lock.entry.{OrderEvent, ReceiptEvent}
import com.lock.function.OrderPayTxDetect
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * 对于订单支付事件，用户支付完成其实并不算完，我们还得确认平台账户上是否到账
  * 而往往这会来自不同的日志信息，所以我们要同时读入两条流的数据来做合并处理
  * connect将两条流,自定义的CoProcessFunction进行处理 [[OrderTwoStreamConnect]]
  *
  * author  Lock.xia
  * Date 2021-01-12
  */
object OrderTwoStreamConnect {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStreamOne: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\OrderLog.csv")

    val payStream: DataStream[OrderEvent] = inputStreamOne.map((k: String) => {
      val dataArr: Array[String] = k.split(",")
      OrderEvent(dataArr(0).toLong, dataArr(1), dataArr(2), dataArr(3).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(1)) {
      override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
    })
      .filter((_: OrderEvent).txId != "") // 只考虑支付信息
      .keyBy((_: OrderEvent).txId)

    val inputStreamTwo: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\ReceiptLog.csv")

    val receiptStream: DataStream[ReceiptEvent] = inputStreamTwo.map((k: String) => {
      val dataArr: Array[String] = k.split(",")
      ReceiptEvent(dataArr(0), dataArr(1), dataArr(2).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(1)) {
      override def extractTimestamp(element: ReceiptEvent): Long = element.eventTime * 1000L
    }).keyBy((_: ReceiptEvent).txId)


    // 匹配失败的通过侧输出流输出
    val unMatchReceiptTag: OutputTag[OrderEvent] = new OutputTag[OrderEvent]("un_match_Receipt")
    val unMatchPayTag: OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("un_match_pays")

    // 两种方式，一种 connect [full join] 另一种 join  / interval join  还有一种 [ union，但是两种流的类型必须一致 ]
    // 可以在连接之前keyBy ==> [CoProcessFunction] ，也可以在连接之后再keyBy ==> [KeyedCoProcessFunction]
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = payStream.connect(receiptStream)
      .process(new OrderPayTxDetect())

    resultStream.print("match_success")
    resultStream.getSideOutput(unMatchPayTag).print("un_match_pay")
    resultStream.getSideOutput(unMatchReceiptTag).print("un_match_receipt")

    env.execute("order pay tx match job connect")
  }

}
