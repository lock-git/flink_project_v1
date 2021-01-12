package com.lock.function

import com.lock.entry.{OrderEvent, OrderResult}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * author  Lock.xia
  * Date 2021-01-12
  */
class OrderPayProcessFunction() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  val orderOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("order_timeout")

  lazy val isCreateState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("create", classOf[Boolean]))
  lazy val isPayState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("pay", classOf[Boolean]))
  lazy val tsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ts", classOf[Long]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    val time: Long = value.eventTime
    val orderId: Long = value.orderId
    val eventType: String = value.eventType

    val isPay: Boolean = isPayState.value()
    val isCreate: Boolean = isCreateState.value()
    val ts: Long = tsState.value()

    if (eventType == "create") {

      if (isPay) { // create 数据延迟过来，pay 已经来过
        out.collect(OrderResult(orderId, "pay success"))
        isPayState.clear()
        tsState.clear()
        ctx.timerService().deleteEventTimeTimer(ts)

      } else { // 订单的 create 第一次过来

        isCreateState.update(true)
        tsState.update(time * 1000L + 15 * 60 * 1000L)
        ctx.timerService().registerEventTimeTimer(time * 1000L + 15 * 60 * 1000L)
      }


    } else if (eventType == "pay") {

      if (isCreate) { // 正常顺序，create -》 pay [ 暂时没有考虑 watermark 延迟，的临界情况，例如，正常 15s 定时器出发，wm=3 ，18s触发，来一个pay为16s，实际已经timeout，但是不会触发定时器]

        if(time * 1000L <= ts){
          out.collect(OrderResult(orderId, "pay success"))
        }else{ // 考虑极端情况
          ctx.output(orderOutputTag,OrderResult(orderId,"payed =================  but order timeout"))
        }

        isCreateState.clear()
        tsState.clear()
        ctx.timerService().deleteEventTimeTimer(ts)

      } else { // 乱序数据，create 在 pay 之后过来

        isPayState.update(true)
        tsState.update(time * 1000L)
        ctx.timerService().registerEventTimeTimer(time * 1000L)
      }


    }

    // 除了以上两种行为，其他暂时不用考虑

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {

    if(isPayState.value()){ // create timeout
      ctx.output(orderOutputTag,OrderResult(ctx.getCurrentKey,"payed but create timeout"))
    }

    if(isCreateState.value()){ // pay timeout
      ctx.output(orderOutputTag,OrderResult(ctx.getCurrentKey,"order ======================= timeout"))
    }

    isCreateState.clear()
    isPayState.clear()
    tsState.clear()


  }
}
