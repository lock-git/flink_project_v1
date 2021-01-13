package com.lock.analysis

import com.lock.entry.{OrderEvent, ReceiptEvent}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.util.Collector

/**
  * author  Lock.xia
  * Date 2021-01-13
  */
class OrderPayTxDetectWithJoin() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{

  // 只能输出匹配上的数据
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect(left,right)
  }
}
