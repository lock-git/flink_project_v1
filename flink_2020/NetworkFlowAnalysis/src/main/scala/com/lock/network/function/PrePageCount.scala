package com.lock.network.function

import com.lock.network.entry.ApacheLogEvent
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * author  Lock.xia
  * Date 2020-12-24
  */
class PrePageCount() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}