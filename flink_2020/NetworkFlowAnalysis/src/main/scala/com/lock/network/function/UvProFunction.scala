package com.lock.network.function

import com.lock.network.entry.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

import scala.collection.mutable


/**
  * author  Lock.xia
  * Date 2020-12-29
  */
class UvProFunction() extends AggregateFunction[UserBehavior,scala.collection.mutable.Set[Long],Long]{
  override def createAccumulator(): mutable.Set[Long] = mutable.Set[Long]()

  override def add(value: UserBehavior, accumulator: mutable.Set[Long]): mutable.Set[Long] = accumulator += value.userId

  override def getResult(accumulator: mutable.Set[Long]): Long = accumulator.size.toLong

  override def merge(a: mutable.Set[Long], b: mutable.Set[Long]): mutable.Set[Long] = a ++ b
}