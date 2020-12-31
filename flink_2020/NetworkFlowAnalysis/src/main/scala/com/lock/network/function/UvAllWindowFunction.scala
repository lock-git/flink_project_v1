package com.lock.network.function

import com.lock.network.entry.{UserBehavior, UvCount}
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * author  Lock.xia
  * Date 2020-12-29
  */
class UvAllWindowFunction() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    val userSet: mutable.HashSet[Long] = new mutable.HashSet[Long]()
    for (elem <- input) {
      userSet += elem.userId
    }
    out.collect(UvCount(window.getEnd, userSet.size.toLong))
  }
}
