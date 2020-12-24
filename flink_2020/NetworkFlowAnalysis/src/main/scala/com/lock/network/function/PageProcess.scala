package com.lock.network.function

import java.sql.Timestamp

import com.lock.network.entry.PageViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * author  Lock.xia
  * Date 2020-12-24
  */
class PageProcess(topN: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

  lazy val urlListState: ListState[PageViewCount] = getRuntimeContext.getListState[PageViewCount](new ListStateDescriptor[PageViewCount]("url_list_state", classOf[PageViewCount]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {

    // 将每一条数据放入listState中
    urlListState.add(value)

    // 注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  // 设置闹钟，闹钟响时，执行输出操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    val pageList: ListBuffer[PageViewCount] = new ListBuffer[PageViewCount]()

    import scala.collection.JavaConversions._
    for (p <- urlListState.get().iterator()) {
      pageList += p
    }

    val sortedList: List[PageViewCount] = pageList.toList.sortWith((_: PageViewCount).count > (_: PageViewCount).count).take(topN)

    val resultStr: StringBuilder = new StringBuilder()
    resultStr.append(s"\nTime : ${new Timestamp(timestamp - 1)} \n")
    for (i <- sortedList.indices) {
      resultStr.append(s"访问量 TOP${i + 1} 的url为：${sortedList(i).url},counts为：${sortedList(i).count}\n")
    }
    resultStr.append("======================================================================================\n\n")

    // 方便测试，sleep
    Thread.sleep(2000)

    out.collect(resultStr.toString())

    // 清空状态
    urlListState.clear()
  }
}