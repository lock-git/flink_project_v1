package com.lock.network.function

import com.lock.network.entry.PvCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * author  Lock.xia
  * Date 2020-12-25
  */
class PvWindowFunction() extends WindowFunction[Long,PvCount,String,TimeWindow]{

  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd,input.head))
  }
}
