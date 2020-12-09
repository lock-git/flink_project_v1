package com.lock.api

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * author  Lock.xia
  * Date 2020-10-19
  */

// 定义样例类，传感器id，时间戳，温度
case class SensorReading(id: String, timestamp: Long, temperature: Double)


object SourceTest {
  def main(args: Array[String]): Unit = {

    // 获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    // 1-从集合获取数据
    val stream1 = env.fromCollection(List(
        SensorReading("sensor_1", 1547718199, 35.8),
        SensorReading("sensor_6", 1547718201, 15.4),
        SensorReading("sensor_7", 1547718202, 6.7),
        SensorReading("sensor_10", 1547718205, 38.1)
      ))

    stream1.print("stream1:").setParallelism(1)

    // 2-从文件中读取数据
    val stream2: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_v1\\src\\main\\resources\\sensor.txt")
    stream2.print("stream2:").setParallelism(1)


    // 3-socket文本流
    // val stream3: DataStream[String] = env.socketTextStream("localhost",7777)


    // 4-从kafka读取数据  FlinkKafkaConsumer011 继承 10 继承 09 继承 SourceFunction
//    val properties: Properties = new Properties()
//    properties.setProperty("bootstrap.servers", "localhost:9092")
//    properties.setProperty("group.id", "consumer-group")
//    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("auto.offset.reset", "latest")
//
//    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))
//    kafkaStream.print("kafkaStream:").setParallelism(1)


    // 5-自定义Source  ===> 实现一个自定义的SourceFunction ==> 自动生成测试数据
    val mySensor: DataStream[SensorReading] = env.addSource(new MySensorSource())
    mySensor.print("My Sensor : ")






    env.execute()


  }
}
