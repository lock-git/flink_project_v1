package com.lock.hotitems

import java.{lang, util}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lock.entry.KafkaLogData
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import redis.clients.jedis.{JedisCluster, Tuple}


/**
  * author  Lock.xia
  * Date 2021-01-11
  */
class LogViewEssayProcess(essayCounts: Int, eventId: String) extends ProcessFunction[DeviceLogEntry, String] {

  lazy val featureJedisCluster: JedisCluster = RedisCluster.getRedisCluster

  override def processElement(value: DeviceLogEntry, ctx: ProcessFunction[DeviceLogEntry, String]#Context, out: Collector[String]): Unit = {

    val deviceId: String = value.deviceId
    val beginTime: String = value.begintime
    val event_id: String = value.eventid
    val eventContent: String = value.eventcontent

    if (StringUtils.isNotBlank(deviceId) && StringUtils.isNotBlank(event_id) && StringUtils.isNotBlank(eventContent)) {
      // if (StringUtils.isNotBlank(deviceId) && StringUtils.isNotBlank(event_id) && eventId.equals(event_id) && StringUtils.isNotBlank(eventContent)) {
      try {
        import scala.collection.JavaConversions._
        val essayClickTimeMap: util.Map[String, java.lang.Double] = new util.HashMap[String, java.lang.Double]()

        val eventJsonObj: JSONObject = JSON.parseObject(eventContent)
        if (eventJsonObj != null && eventJsonObj.containsKey("ctr")) {

          val ctrObjStr: String = eventJsonObj.get("ctr").toString
          if (StringUtils.isNotBlank(ctrObjStr)) {
            val dataCtrList: List[KafkaLogData.Ctr] = JSON.parseArray(ctrObjStr, classOf[KafkaLogData.Ctr]).toList

            // 移除缓存中用户阅读时间超过一个月的文章
            val dataTuple: util.Set[Tuple] = featureJedisCluster.zrangeWithScores(String.format("uve20:%s", deviceId), 0, -1)
            if (!dataTuple.isEmpty) {
              val beforeMonthTs: Long = System.currentTimeMillis() - (30 * 24 * 60 * 60 * 1000L)
              for (t <- dataTuple) {
                val essay_id_original: String = t.getElement
                val ts: Long = t.getScore.toLong
                if (ts > beforeMonthTs) {
                  essayClickTimeMap.put(essay_id_original, java.lang.Double.valueOf(ts))
                }
              }
            }

            // 新加入的文章
            if (dataCtrList.nonEmpty) {
              for (dataCtr <- dataCtrList) {
                if ("recommend_goods".equals(dataCtr.reality_type) && "1".equals(dataCtr.exposure_times) && StringUtils.isNotBlank(dataCtr.reality_id)) {
                  // if ("essay_detail".equals(dataCtr.reality_type) && "1".equals(dataCtr.exposure_times) && StringUtils.isNotBlank(dataCtr.reality_id)) {
                  essayClickTimeMap.put(dataCtr.reality_id, java.lang.Double.valueOf(beginTime))
                }
              }
            }

            if (!essayClickTimeMap.isEmpty) { // 用户近 1个月  最后N篇  阅读文章id
              val topNMap: Map[String, lang.Double] = essayClickTimeMap.toList.sortWith((a: (String, lang.Double), b: (String, lang.Double)) => a._2 > b._2).take(essayCounts).toMap
              featureJedisCluster.del(String.format("uve20:%s", deviceId))
              featureJedisCluster.zadd(String.format("uve20:%s", deviceId), topNMap)
              featureJedisCluster.expire(String.format("uve20:%s", deviceId), 30 * 60)
              essayClickTimeMap.clear()
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

  /*override def processElement(value: DeviceLogEntry, ctx: ProcessFunction[DeviceLogEntry, String]#Context, out: Collector[String]): Unit = {

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
                if (dataTuple != null && dataTuple.size() > essayCounts) {
                  val endIndex: Long = (dataTuple.size() - essayCounts - 1).toLong
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
  }*/

}
