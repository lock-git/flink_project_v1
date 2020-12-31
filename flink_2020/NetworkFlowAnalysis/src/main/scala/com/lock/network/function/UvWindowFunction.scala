package com.lock.network.function

import com.lock.network.entry.UvCount
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * author  Lock.xia
  * Date 2020-12-29
  */
class UvWindowFunction() extends AllWindowFunction[Long, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UvCount]): Unit = {

    out.collect(UvCount(window.getEnd, input.head))

  }
}
