package com.lock.hotitems.analysis

import java.sql.Timestamp

import com.lock.hotitems.entry.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * author  Lock.xia
  * Date 2020-12-18
  */
object HotItems {

  def main(args: Array[String]): Unit = {

    // 创建一个流式处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // 设置时间语义 eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据
    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    // 对数据进行转化
    val dataStream: DataStream[UserBehavior] = inputStream.map((k: String) => {
      val dataArr: Array[String] = k.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
    })
      .assignAscendingTimestamps((_: UserBehavior).timestamp * 1000L) //因为数据是文件中进行处理过的升序数据，没有乱序情况，直接指定时间戳即可，不用watermark

    // 对数据进行开窗统计，为了避免，窗口时间过短，不同窗口的数据混合在一起，需要在数据中带出窗口的信息，因此需要全窗口函数【windowFunction】
    // .aggregate() 增量聚合函数，来一条处理一条，对性能上比较好，缺点是拿不到窗口信息。故传入两个参数，第一个是预聚合函数，第二个是全窗口函数，拿到窗口信息

    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv") // 过滤pv信息
      .keyBy("itemId") // 按照 itemId 进行分组聚合
      .timeWindow(Time.hours(1), Time.minutes(5)) //定义滑动窗口[实际滑动窗口很消耗性能]：  窗口大小：1h ，滑动步长：5min
      .aggregate(new CountAgg(), new ItemCountWindowResult()) // 定义窗口计算规则

    // 对窗口聚合结果按照窗口分组，并做排序取TopN输出
    val resultStream: DataStream[String] = aggStream
      .keyBy("WindowEnd")
      .process(new TopNHotItems(5)) // // 只有 processFunction 可以设置定时器

    //将测试结果打印输出
    resultStream.print("result")

    env.execute("hot item job")

  }

}

/**
  * 自义定预聚合函数类
  */
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  // 用于 session 窗口 merge
  override def merge(a: Long, b: Long): Long = a + b
}


// 综合 window 的信息  注意： Tuple 是一个java类型, key 是一个 keyedStream Tuple
class ItemCountWindowResult extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]() {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd: Long = window.getEnd
    val count: Long = input.iterator.next() // 因为只有一个数，直接获取，或者get第0个元素即可
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}


/**
  * 自定义 KeyedProcessFunction  函数类
  * @param n topN
  *
  */
class TopNHotItems(n: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  // 定义一个listState，用来保存当前窗口所有的count值
  lazy val itemCountListState: ListState[ItemViewCount] = getRuntimeContext.getListState[ItemViewCount](new ListStateDescriptor[ItemViewCount]("item-state-list", classOf[ItemViewCount]))

  //  必须实现的函数，每来一条数据做一个怎么的操作，在此函数中进行
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {

    // 每来一条数据添加一条
    itemCountListState.add(value)
    // 注册定时器，设置时间，到点触发 [ 程序看起来是每来一条数据，注册一次，实际 flink 对于重复注册，会忽略，不会对性能有影响 ]
    ctx.timerService().registerEventTimeTimer(value.WindowEnd + 100)

  }

  // 开启闹钟模式，到点执行
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 遍历 listState 中的值[java类型]，放入到一个 listBuffer [scala] 中
    val itemCountList: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]()
    import scala.collection.JavaConversions._
    for (ic <- itemCountListState.get()) {
      itemCountList += ic
    }

    // 按照 count值排序 通过Ordering类进行排序
    val sortedStream: List[ItemViewCount] = itemCountList.toList.sortBy((_: ItemViewCount).count)(Ordering.Long.reverse).take(n)

    // 输出
    val resultStr: StringBuilder = new StringBuilder()

    resultStr.append("本次窗口的时间：").append(new Timestamp(timestamp - 100)).append("\n")
    for (i <- sortedStream.indices) {
      val singleSort: ItemViewCount = sortedStream(i)
      resultStr.append(s"TOP ${i + 1} :")
        .append(" itemId 为：").append(singleSort.ItemId)
        .append(" count值 为：").append(singleSort.count)
        .append("\n")
    }
    resultStr.append(" =============================== \n\n")

    out.collect(resultStr.toString())

    // 为了测试方便观察，sleep一下,实际线上不用如此
    Thread.sleep(1000L)

    // 处触发完之后，清空状态
    itemCountListState.clear()

  }
}