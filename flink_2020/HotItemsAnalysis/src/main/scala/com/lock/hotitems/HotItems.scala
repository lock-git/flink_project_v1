package com.lock.hotitems

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * author  Lock.xia
  * Date 2020-12-18
  */
object HotItems {

  def main(args: Array[String]): Unit = {

    // 创建一个流式处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 设置时间语义 eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据
    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    // 对数据进行转化
    val dataStream: DataStream[UserBehavior] = inputStream.map((k: String) => {
      val dataArr: Array[String] = k.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2), dataArr(3).toLong)
    })
      .assignAscendingTimestamps((_: UserBehavior).timestamp * 1000L) //因为数据是文件中进行处理过的升序数据，没有乱序情况，直接指定时间戳即可，不用watermark

    // 对数据进行开窗统计，为了避免，窗口时间过短，不同窗口的数据混合在一起，需要在数据中带出窗口的信息，因此需要全窗口函数【windowFunction】
    // .aggregate() 增量聚合函数，来一条处理一条，对性能上比较好，缺点是拿不到窗口信息。故传入两个参数，第一个是预聚合函数，第二个是全窗口函数，拿到窗口信息

    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv") // 过滤pv信息
      .keyBy("itemId") // 按照 itemId 进行分组聚合
      .timeWindow(Time.hours(1), Time.milliseconds(5)) //定义滑动窗口[实际滑动窗口很消耗性能]：  窗口大小：1h ，滑动步长：5min
      .aggregate(new CountAgg(), new ItemCountWindowResult()) // 定义窗口计算规则

    // 对窗口聚合结果按照窗口分组，并做排序取TopN输出
    val resultStream: DataStream[String] = aggStream
      .keyBy("windowEnd")
      .process(new TopNHotItems(5)) // // 只有 processFunction 可以设置定时器

    resultStream.print()


    env.execute("hot item job")

  }


}

// 自义定预聚合函数
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
    val windowEnd: Long = window.getEnd()
    val count: Long = input.iterator.next() // 因为只有一个数，直接获取，或者get第0个元素即可
    out.collect(ItemViewCount(itemId, windowEnd, count))

  }
}

// 自定义 KeyedProcessFunction
class TopNHotItems(n: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  // 定义一个listState，用来保存当前窗口所有的count值


  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {

  }
}