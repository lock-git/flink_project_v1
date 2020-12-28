package com.lock.network.function

import org.apache.flink.api.common.functions.AggregateFunction

/**
  * author  Lock.xia
  * Date 2020-12-25
  */
class PrePVAcc() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + value._2

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
