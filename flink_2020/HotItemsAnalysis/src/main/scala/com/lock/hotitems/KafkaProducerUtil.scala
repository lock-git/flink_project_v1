package com.lock.hotitems

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

/**
  * author  Lock.xia
  * Date 2020-12-23
  */
object KafkaProducerUtil {
  def main(args: Array[String]): Unit = {

    writeToKafkaWithTopic("hot_items")

  }

  def writeToKafkaWithTopic(topic: String): Unit = {

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.16.249.59:9092") // 测试集群
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 创建一个 kafkaProducer ，用它来发送数据
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)

    // 从文件中读取数据，逐条发送
    val bufferedSource: BufferedSource = io.Source.fromFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    for (line <- bufferedSource.getLines()) {
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }

    producer.close()
  }

}
