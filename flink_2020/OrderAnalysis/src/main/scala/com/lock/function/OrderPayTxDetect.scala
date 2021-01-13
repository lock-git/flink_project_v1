package com.lock.function

import com.lock.entry.{OrderEvent, ReceiptEvent}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * author  Lock.xia
  * Date 2021-01-13
  */
class OrderPayTxDetect() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

  // 定义状态 payState - receiptState
  lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))
  lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

  // 侧输出流
  val unMatchReceiptTag: OutputTag[OrderEvent] = new OutputTag[OrderEvent]("un_match_Receipt")
  val unMatchPayTag: OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("un_match_pays")

  override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val receipt: ReceiptEvent = receiptState.value()

    if (receipt != null) { // 匹配成功  =》 清空状态

      out.collect((value, receipt))
      receiptState.clear()

    } else { // 没有匹配到账单信息[receipt]  =》 更新状态，注册定时器[ 等待5s ]

      payState.update(value)
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5 * 1000L)
    }

  }

  override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val pay: OrderEvent = payState.value()

    if (pay != null) { // 匹配成功  =》 清空状态

      out.collect((pay, value))
      payState.clear()

    } else { // 没有匹配到用户支付信息[pay]  =》 更新状态，注册定时器[ 等待3s ]

      receiptState.update(value)
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 3 * 1000L)
    }

  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    // 如果有 pay，receipt延迟
    if (payState.value() != null) {
      ctx.output(unMatchReceiptTag, payState.value())
    }

    // 如果有 receipt，pay延迟
    if (receiptState.value() != null) {
      ctx.output(unMatchPayTag, receiptState.value())
    }

    // 清空状态
    payState.clear()
    receiptState.clear()
  }
}