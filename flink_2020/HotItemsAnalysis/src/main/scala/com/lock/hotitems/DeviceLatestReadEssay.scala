package com.lock.hotitems

import java.util

import com.lock.entry.KafkaLogData
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.collection.mutable.ListBuffer


/**
  * author  Lock.xia
  * Date 2021-01-11
  */
object DeviceLatestReadEssay {

  val topicName = "LOG_TOPIC_MOTO"

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](topicName, new SimpleStringSchema(), MotoKafkaUtils.getKafkaPropertiesTest("DeviceLatestReadEssay_test_v2")))

    val logDataStream: DataStream[KafkaLogData] = kafkaStream.map(new DeviceLogDataMap())

    val filterStream: DataStream[KafkaLogData] = logDataStream.filter((k: KafkaLogData) => StringUtils.isNotBlank(k.deviceid))

    val logStream: DataStream[DeviceLogEntry] = filterStream.flatMap((k: KafkaLogData) => {
      val deviceLogList = new ListBuffer[DeviceLogEntry]()
      import scala.collection.JavaConversions._
      val deviceId: String = k.deviceid
      val platform: String = k.plateform
      val version: String = k.version
      val channel: String = k.channel
      val ip: String = k.ip
      val logs: util.List[KafkaLogData.Logs] = k.logs
      for (log: KafkaLogData.Logs <- logs) {
        deviceLogList += DeviceLogEntry(deviceId, platform, version, channel, ip, log.eventid, log.eventcontent, log.areacode, log.begintime)
      }
      deviceLogList
    })

    val exceptionData: DataStream[String] = logStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DeviceLogEntry](Time.seconds(1)) {
      override def extractTimestamp(element: DeviceLogEntry): Long = element.begintime.toLong
    }).process(new LogViewEssayProcess(20, "A_10209001002"))

    exceptionData.print("exception")

    logStream.print("log")
    env.execute("device latest view essay")
  }

}


