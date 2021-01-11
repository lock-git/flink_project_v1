package com.lock.hotitems

import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import redis.clients.jedis.{JedisCluster, Tuple}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.lock.entry.KafkaLogData
import java.util


/**
  * author  Lock.xia
  * Date 2021-01-11
  */
class LogViewEssayProcess(essayCount: Int, eventId: String) extends ProcessFunction[DeviceLogEntry, String] {

  lazy val featureJedisCluster: JedisCluster = RedisCluster.getRedisCluster

  override def processElement(value: DeviceLogEntry, ctx: ProcessFunction[DeviceLogEntry, String]#Context, out: Collector[String]): Unit = {

    val deviceId: String = value.deviceId
    val beginTime: String = value.begintime
    val event_id: String = value.eventid
    val eventContent: String = value.eventcontent

    if (StringUtils.isNotBlank(deviceId) && StringUtils.isNotBlank(eventId) && StringUtils.isNotBlank(eventContent)) {
//    if (StringUtils.isNotBlank(deviceId) && StringUtils.isNotBlank(eventId) && eventId.equals(event_id) && StringUtils.isNotBlank(eventContent)) {
      try {
        import scala.collection.JavaConversions._
        val essayClickTimeMap: util.Map[String, java.lang.Double] = new util.HashMap[String, java.lang.Double]()
        val eventJsonObj: JSONObject = JSON.parseObject(eventContent)
        if (eventJsonObj != null && eventJsonObj.containsKey("ctr")) {
          val ctrObjStr: String = eventJsonObj.get("ctr").toString
          if (StringUtils.isNotBlank(ctrObjStr)) {
            val dataCtrList: List[KafkaLogData.Ctr] = JSON.parseArray(ctrObjStr, classOf[KafkaLogData.Ctr]).toList
            if (dataCtrList.nonEmpty) {
              for (dataCtr <- dataCtrList) {
                if ("recommend_goods".equals(dataCtr.reality_type) && "1".equals(dataCtr.exposure_times) && StringUtils.isNotBlank(dataCtr.reality_id)) {
//                if ("essay_detail".equals(dataCtr.reality_type) && "1".equals(dataCtr.exposure_times) && StringUtils.isNotBlank(dataCtr.reality_id)) {
                  essayClickTimeMap.put(dataCtr.reality_id, java.lang.Double.valueOf(beginTime))
                }
              }
              if (!essayClickTimeMap.isEmpty) {
                featureJedisCluster.zadd(String.format("uve20:%s", deviceId), essayClickTimeMap)
                val dataTuple: util.Set[Tuple] = featureJedisCluster.zrangeWithScores(String.format("uve20:%s", deviceId), 0, -1)
                if (dataTuple != null && dataTuple.size() > essayCount) {
                  val endIndex: Long = (dataTuple.size() - essayCount - 1).toLong
                  featureJedisCluster.zremrangeByRank(String.format("uve20:%s", deviceId), 0, endIndex)
                }
                featureJedisCluster.expire(String.format("uve20:%s", deviceId), 30 * 60)
              }
            }
          }
        }
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          out.collect(s"exception data event_content: $eventContent \n $ex")
      }
    }
  }
}
