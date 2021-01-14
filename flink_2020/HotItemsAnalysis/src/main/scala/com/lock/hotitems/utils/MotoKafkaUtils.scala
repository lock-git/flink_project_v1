package com.lock.hotitems.utils

import java.util.Properties

/**
  * author  Lock.xia
  * Date 2021-01-11
  */
object MotoKafkaUtils {

  def getKafkaProperties(groupID: String): Properties = {
    val properties: Properties = new Properties()
    properties.setProperty("metadata.broker.list", "172.16.1.111:9092,172.16.1.113:9092,172.16.1.115:9092")
    properties.setProperty("bootstrap.servers", "172.16.1.111:9092,172.16.1.113:9092,172.16.1.115:9092")
    properties.setProperty("group.id", groupID)
    properties.setProperty("max.partition.fetch.bytes", "50000000")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    properties.setProperty("enable.auto.commit", "true")
    properties
  }


  def getKafkaPropertiesTest(groupID: String): Properties = {
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.16.249.59:9092")
    properties.setProperty("group.id", groupID)
    properties.setProperty("max.partition.fetch.bytes", "50000000")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("enable.auto.commit", "false")
    properties
  }

}
