package com.lock.entry

/**
  * author  Lock.xia
  * Date 2021-01-12
  */
object OrderModel {

}

case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

case class OrderResult(orderId: Long, resultMsg: String)