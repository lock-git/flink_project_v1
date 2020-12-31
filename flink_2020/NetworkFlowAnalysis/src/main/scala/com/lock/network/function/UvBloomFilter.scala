package com.lock.network.function

import com.lock.network.entry.UvCount
import com.lock.network.util.{BloomFilter, RedisCluster}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.JedisCluster

/**
  * author  Lock.xia
  * Date 2020-12-30
  */
class UvBloomFilter() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

  // 定一个redis连接器
  lazy val jedis: JedisCluster = RedisCluster.getRedisCluster

  // 定义位图的大小
  lazy val mapBitSize: Int = 1 << 30


  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {

    val redisKey: String = context.window.getEnd.toString

    var count = 0L
    if (jedis.hget("user_behavior:uv", redisKey) != null) {
      count = jedis.hget("user_behavior:uv", redisKey).toLong
    }

    // 由于窗口中来一条数据处理一条，elements 只有一条数据
    val userId: String = elements.head._2.toString

    val isExist: Boolean = BloomFilter.exists(userId)

    if (!isExist) {
      count = count + 1
      jedis.hset("user_behavior:uv", redisKey, count.toString)
      out.collect(UvCount(redisKey.toLong, count))
    } else {
      out.collect(UvCount(redisKey.toLong, count))
    }
  }
}


