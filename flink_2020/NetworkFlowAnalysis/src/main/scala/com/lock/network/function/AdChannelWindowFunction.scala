package com.lock.network.function

import java.sql.Timestamp

import com.lock.network.entry.MarketingCountView
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * author  Lock.xia
  * Date 2021-01-04
  */
class AdChannelWindowFunction() extends WindowFunction[Long, MarketingCountView, (String, String), TimeWindow] {

  override def apply(key: (String, String), window: TimeWindow, input: Iterable[Long], out: Collector[MarketingCountView]): Unit = {

    val windowStart: String = new Timestamp(window.getStart).toString
    val windowEnd: String = new Timestamp(window.getEnd).toString
    out.collect(MarketingCountView(windowStart, windowEnd, key._2, key._1, input.head))
  }
}
