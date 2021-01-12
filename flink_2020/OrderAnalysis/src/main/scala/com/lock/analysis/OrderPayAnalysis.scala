package com.lock.analysis

import com.lock.entry.{OrderEvent, OrderResult}
import com.lock.function.{OrderPatternSelect, OrderTimeoutSelect}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * 为了让用户更有紧迫感从而提高支付转化率，同时也为了防范订单支付环节的安全风险，
  * 电商网站往往会对订单状态进行监控，设置一个失效时间（比如15分钟），
  * 如果下单后一段时间仍未支付，订单就会被取消
  *
  * author  Lock.xia
  * Date 2021-01-11
  */
object OrderPayAnalysis {
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

    // 1.pattern 定义模式匹配 ===> 15min 内创建订单且成功支付
    val orderPattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("create").where((_: OrderEvent).eventType == "create")
      .followedBy("pay").where((_: OrderEvent).eventType == "pay")
      .within(Time.minutes(15))

    // 2.CEP 将匹配模式应用在按照OrderId分组的流上
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(dataStream.keyBy((_: OrderEvent).orderId), orderPattern)


    // 3.定义一个侧输出流标签，用来标明超时事件的侧输出流
    val orderTimeoutTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("order timeout")

    // 4.调用select方法，提取匹配事件
    val resultStream: DataStream[OrderResult] = patternStream
      .select(orderTimeoutTag, new OrderTimeoutSelect(), new OrderPatternSelect())

    // 从侧输出流获取超时订单
    val timeoutStream: DataStream[OrderResult] = resultStream.getSideOutput(orderTimeoutTag)


    resultStream.print("success")
    timeoutStream.print("timeout")

    env.execute("order pattern")
  }
}
