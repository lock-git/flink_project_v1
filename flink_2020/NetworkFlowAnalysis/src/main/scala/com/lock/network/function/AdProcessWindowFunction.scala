package com.lock.network.function

import java.sql.Timestamp

import com.lock.network.entry.{MarketingCountView, MarketingUserBehavior}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * author  Lock.xia
  * Date 2021-01-04
  */
class AdProcessWindowFunction() extends ProcessWindowFunction[MarketingUserBehavior, MarketingCountView, (String, String), TimeWindow] {

  override def process(key: (String, String), context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[MarketingCountView]): Unit = {

    val windowStart: String = new Timestamp(context.window.getStart).toString
    val windowEnd: String = new Timestamp(context.window.getEnd).toString
    out.collect(MarketingCountView(windowStart, windowEnd, key._2, key._1, elements.size))
  }


}
