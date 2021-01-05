package com.lock.network.function

import java.sql.Timestamp

import com.lock.network.entry.CountByProvince
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * author  Lock.xia
  * Date 2021-01-04
  */
class AdClickWindowFunction() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {

  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {

    val timeEnd: String = new Timestamp(window.getEnd).toString

//    out.collect(CountByProvince(timeEnd, key.split("-")(0), input.head))
    out.collect(CountByProvince(timeEnd, key, input.head))

  }
}
