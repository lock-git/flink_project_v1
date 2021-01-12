package com.lock.function

import java.sql.Timestamp
import java.util

import com.lock.entry.{OrderEvent, OrderResult}
import org.apache.flink.cep.PatternSelectFunction

/**
  * author  Lock.xia
  * Date 2021-01-12
  */
class OrderPatternSelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payOrderInfo: OrderEvent = map.get("pay").iterator().next()
    OrderResult(payOrderInfo.orderId, s"pay success at ${new Timestamp(payOrderInfo.eventTime)}")
  }
}
