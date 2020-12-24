package com.lock.network.function

import java.sql.Timestamp
import java.util

import com.lock.network.entry.PageViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * author  Lock.xia
  * Date 2020-12-24
  */
class PageCountProcessFunction(topN: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

  lazy val pageListState: ListState[PageViewCount] = getRuntimeContext.getListState[PageViewCount](new ListStateDescriptor[PageViewCount]("page_list_state", classOf[PageViewCount]))


  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {

    pageListState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    val stateData: util.Iterator[PageViewCount] = pageListState.get().iterator()

    val page_lsit: ListBuffer[PageViewCount] = new ListBuffer[PageViewCount]()

    import scala.collection.JavaConversions._
    for (p <- stateData) {
      page_lsit += p
    }

    val sortedList: List[PageViewCount] = page_lsit.toList.sortBy((_: PageViewCount).count)(Ordering.Long.reverse).take(topN)

    val resultStr: StringBuilder = new StringBuilder()
    resultStr.append(s"time: ${new Timestamp(timestamp - 1)} \n")
    for (i <- sortedList.indices) {
      resultStr.append(s"TOP ${i + 1} : url===>${sortedList(i).url},counts===>${sortedList(i).count}\n")
    }
    resultStr.append("==================================================================\n\n\n")

    out.collect(resultStr.toString())
    pageListState.clear()
  }
}
