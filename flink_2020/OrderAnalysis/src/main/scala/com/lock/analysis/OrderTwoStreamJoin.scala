package com.lock.analysis

import com.lock.entry.{OrderEvent, ReceiptEvent}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * join ===》[inner] JoinedStream：必须开窗 ===> where() ---> 指定流中key的选取
  *
  * intervalJoin ===》[时间的上下界扫扫描]
  *
  * author  Lock.xia
  * Date 2021-01-13
  */
object OrderTwoStreamJoin {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStreamOne: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\OrderLog.csv")

    val payStream: KeyedStream[OrderEvent, String] = inputStreamOne.map((k: String) => {
      val dataArr: Array[String] = k.split(",")
      OrderEvent(dataArr(0).toLong, dataArr(1), dataArr(2), dataArr(3).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(1)) {
      override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
    })
      .filter((_: OrderEvent).txId != "") // 只考虑支付信息
      .keyBy((_: OrderEvent).txId)


    val inputStreamTwo: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\ReceiptLog.csv")

    val receiptStream: KeyedStream[ReceiptEvent, String] = inputStreamTwo.map((k: String) => {
      val dataArr: Array[String] = k.split(",")
      ReceiptEvent(dataArr(0), dataArr(1), dataArr(2).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(1)) {
      override def extractTimestamp(element: ReceiptEvent): Long = element.eventTime * 1000L
    }).keyBy((_: ReceiptEvent).txId)


    // 使用 intervalJoin 连接两条流,但是只能输出匹配上的数据，异常数据无法输出
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = payStream
      .intervalJoin(receiptStream)
      .between(Time.seconds(-3), Time.seconds(5))
      .process(new OrderPayTxDetectWithJoin())

    resultStream.print("match success")

    env.execute(" match success with intervalJoin ")

  }
}
