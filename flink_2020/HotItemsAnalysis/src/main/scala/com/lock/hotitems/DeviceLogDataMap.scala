package com.lock.hotitems

import com.lock.entry.{KafkaLogData, KafkaLogMsg}
import org.apache.flink.api.common.functions.MapFunction
import com.alibaba.fastjson.JSON

/**
  * author  Lock.xia
  * Date 2021-01-11
  */
class DeviceLogDataMap() extends MapFunction[String, KafkaLogData] {

  override def map(value: String): KafkaLogData = {
    var returnLogData: KafkaLogData = new KafkaLogData()
    try {
      val logMsg: KafkaLogMsg = JSON.parseObject(value, classOf[KafkaLogMsg])
      try {
        returnLogData = JSON.parseObject(logMsg.getData, classOf[KafkaLogData])
        returnLogData.setIp(logMsg.getIp)
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          println("logMsg.getData:" + logMsg.getData)
          returnLogData.setDeviceid("")
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        println("KafkaLogMsg:" + value)
        returnLogData.setDeviceid("")
    }
    returnLogData
  }
}
