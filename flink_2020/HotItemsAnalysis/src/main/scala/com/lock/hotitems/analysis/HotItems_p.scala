package com.lock.hotitems.analysis

import java.sql.Timestamp

import com.lock.hotitems.entry.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  *
  * 需求描述：每五分钟输出一次，最近一个小时热门文章的topN
  * 技术描述：flink的滑动窗口
  *
  * author  Lock.xia
  * Date 2020-12-22
  */
object HotItems_p {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    """ 时间语义 """
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    """ 接入数据源 """
    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    """ 转化数据，设置watermark """
    val inStream: DataStream[UserBehavior] = inputStream.map((k: String) => {
      val dataArr: Array[String] = k.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(0).toLong, dataArr(0).toLong, dataArr(0), dataArr(0).toLong)
    }).assignAscendingTimestamps((_: UserBehavior).timestamp * 1000)

    """ itemId 分组，开窗，预聚合，包装 window 信息 """
    val aggStream: DataStream[ItemViewCount] = inStream.keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new Agg_Pre(), new CountWindow())

    """ 窗口分组，counts 排序， 去topN """
    val resultStream: DataStream[String] = aggStream.keyBy("WindowEnd")
      .process(new SortedCountsItem(5))

    resultStream.print("resultStream")

    env.execute("item count top n")

  }

}

// todo  自定义预聚合函数类
class Agg_Pre() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}


// todo  自定义全窗口函数类
class CountWindow() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val windowEnd: Long = window.getEnd
    val counts: Long = input.iterator.next() // 因为只有一条数据，也可以获取第0个元素
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    out.collect(ItemViewCount(itemId, windowEnd, counts))
  }
}


// todo 自定义 KeyedProcessFunction 函数类
class SortedCountsItem(topN:Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{

  """ 初始化一个listState来记录同一个窗口的数据 """
  lazy val itemListState: ListState[ItemViewCount] = getRuntimeContext.getListState[ItemViewCount](new ListStateDescriptor[ItemViewCount]("item_state_count",classOf[ItemViewCount]))

  """ 每来一条数据，在此函数中进行一次处理 """
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {

    itemListState.add(value)
    ctx.timerService().registerEventTimeTimer(value.WindowEnd + 100)

  }

  """ 闹钟触发， 窗口排序输出 """
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    val itemList: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]()

    import scala.collection.JavaConversions._
    for(t <- itemListState.get().iterator()){
      itemList += t
    }

    val sortedItemList: List[ItemViewCount] = itemList.toList.sortBy((_: ItemViewCount).count)(Ordering.Long.reverse).take(topN)

    val resultStr: StringBuilder = new StringBuilder()
    resultStr.append(s"\nwindow_time: ${new Timestamp(timestamp - 100)} 的topN为：\n")
    for (n <- sortedItemList.indices) {
      resultStr.append(s"TOP${n + 1} : itemid为 ${sortedItemList(n).ItemId},counts为 ${sortedItemList(n).count} \n")
    }
    resultStr.append("=========================================== \n\n")

    """ 方便测试 sleep """
    Thread.sleep(1000)

    out.collect(resultStr.toString())

    """ 清空状态 """
    itemListState.clear()
  }
}

