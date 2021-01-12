package com.lock.function

import java.sql.Timestamp
import java.util

import com.lock.entry.{OrderEvent, OrderResult}
import org.apache.flink.cep.PatternTimeoutFunction

/**
  * author  Lock.xia
  * Date 2021-01-12
  */
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent,OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderInfo: OrderEvent = map.get("create").iterator().next()
    OrderResult(timeoutOrderInfo.orderId,s"order time out at ${new Timestamp(l)}")
  }
}
