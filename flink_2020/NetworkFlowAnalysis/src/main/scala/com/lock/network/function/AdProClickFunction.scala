package com.lock.network.function

import org.apache.flink.api.common.functions.AggregateFunction

/**
  * author  Lock.xia
  * Date 2021-01-04
  */
class AdProClickFunction() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
