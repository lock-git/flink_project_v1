package com.lock.network.function

import java.sql.Timestamp

import com.lock.network.entry.PageViewCount
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * author  Lock.xia
  * Date 2020-12-24
  */
class PageCountProcessFunction(topN: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {


  // 定义一个 mapState ，通过key更新数据
  lazy val pageMapState: MapState[String, Long] = getRuntimeContext.getMapState[String, Long](new MapStateDescriptor[String, Long]("page_map_state", classOf[String], classOf[Long]))


  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {

    pageMapState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    // 时间到了延迟时间，清空状态
    if (timestamp == (ctx.getCurrentKey + 60 * 1000)) {
      pageMapState.clear()
      return
    }

    val page_lsit: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]()

    import scala.collection.JavaConversions._
    for (p <- pageMapState.entries()) {
      page_lsit += ((p.getKey, p.getValue))
    }

    val sortedList: List[(String, Long)] = page_lsit.toList.sortBy((_: (String, Long))._2)(Ordering.Long.reverse).take(topN)

    val resultStr: StringBuilder = new StringBuilder()
    resultStr.append(s"time: ${new Timestamp(timestamp - 1)} \n")
    for (i <- sortedList.indices) {
      resultStr.append(s"TOP ${i + 1} : url===>${sortedList(i)._1},counts===>${sortedList(i)._2}\n")
    }
    resultStr.append("==================================================================\n\n\n")

    out.collect(resultStr.toString())

  }
}
