package com.lock.hotitems

import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
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
    //  def getkafkaParam() = {
    //    var kafkaParam = new scala.collection.immutable.HashMap[String, String]()
    //    kafkaParam += ("metadata.broker.list" -> "172.16.1.111:9092,172.16.1.113:9092,172.16.1.115:9092")
    //    kafkaParam += ("bootstrap.servers" -> "172.16.1.111:9092,172.16.1.113:9092,172.16.1.115:9092")
    //    kafkaParam += ("group.id" -> "SimilarArticle_test_v2")
    //    kafkaParam += ("max.partition.fetch.bytes" -> "50000000")
    //    kafkaParam += ("auto.offset.reset" -> "latest")
    //    kafkaParam += ("key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
    //    kafkaParam += ("value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
    //    kafkaParam += ("enable.auto.commit" -> "true")
    //    kafkaParam
    //  }
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.16.249.59:9092")
    properties.setProperty("group.id", "item-group2")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("enable.auto.commit", "false")

    // spark
    // val message: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
    //      LocationStrategies.PreferConsistent,
    //      ConsumerStrategies.Subscribe(topicSet, kafkaParams))

    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hot_items", new SimpleStringSchema(), properties))

    """ 转化数据，设置watermark """
    val inStream: DataStream[UserBehavior] = inputStream.map((k: String) => {
      val dataArr: Array[String] = k.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(0).toLong, dataArr(0).toLong, dataArr(0), dataArr(0).toLong)
    }).assignAscendingTimestamps((_: UserBehavior).timestamp * 1000) // 针对升序api中，watermark 实际是窗口中最大的时间戳 - 1 millisecond

    """ itemId 分组，开窗，预聚合，包装 window 信息 """
    val aggStream: DataStream[ItemViewCount] = inStream.keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new Agg_Pre(), new CountWindow()) // windowStream 的api中，不能传入richFunction，java的底层代码中，报异常“This aggregate function cannot be a RichFunction.”

    """ 窗口分组，counts 排序， 去topN """
    val resultStream: DataStream[String] = aggStream.keyBy("WindowEnd")
      .process(new SortedCountsItem(5))

    resultStream.print("resultStreamP2")

    env.execute("item count top n p2")

  }
}


// 扩展一个自定义预聚合函数： 平均值 ==> 自定义的状态为 (sum,count)
class avgAggCount() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = (accumulator._1 + value.timestamp, accumulator._2 + 1)

  override def getResult(accumulator: (Long, Int)): Double = accumulator._1.toDouble / accumulator._2.toDouble

  """ 只针对 session 窗口 """

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)
}