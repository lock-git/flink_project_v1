package com.lock.network.function

import com.lock.network.entry.MarketingUserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * author  Lock.xia
  * Date 2021-01-04
  */
class AdProAggFunction() extends AggregateFunction[MarketingUserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: MarketingUserBehavior, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}