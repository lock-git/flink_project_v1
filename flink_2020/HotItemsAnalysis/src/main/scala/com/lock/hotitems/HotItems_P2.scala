package com.lock.hotitems

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * author  Lock.xia
  * Date 2020-12-23
  */
object HotItems_P2 {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    """ 时间语义 """
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    """ 接入 kafka 数据源 """

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.16.249.59:9092")
    properties.setProperty("group.id", "item-group1")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")


    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hot_items",new SimpleStringSchema(),properties))

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

    resultStream.print("resultStreamP2")

    env.execute("item count top n p2")

  }
}
