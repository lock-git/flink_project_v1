package com.lock.network.util

import java.util

import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * author  Lock.xia
  * Date 2020-12-30
  */
object RedisCluster {
  private val poolConf = new JedisPoolConfig
  poolConf.setMaxTotal(100)
  poolConf.setMaxIdle(20)
  poolConf.setMaxWaitMillis(20)
  val jedisClusterNodes = new util.HashSet[HostAndPort]
  jedisClusterNodes.add(new HostAndPort("172.16.250.194", 6387))
  jedisClusterNodes.add(new HostAndPort("72.16.250.5", 6384))
  jedisClusterNodes.add(new HostAndPort("172.16.250.5", 6385))
  jedisClusterNodes.add(new HostAndPort("172.16.251.94", 6383))
  jedisClusterNodes.add(new HostAndPort("172.16.251.94", 6384))
  jedisClusterNodes.add(new HostAndPort("172.16.250.194", 6388))

  def getRedisCluster: JedisCluster = {
    new JedisCluster(jedisClusterNodes, poolConf)
  }

  def main(args: Array[String]): Unit = {
    val jedis: JedisCluster = getRedisCluster
    val map: mutable.HashMap[String, String] = mutable.HashMap[String, String]()
    map.put("colname1", "value1")
    map.put("colname2", "value2")
    map.put("colname3", "value3")
    //key值为库名:表名
    jedis.hset("Db:Table:id", "name", "daniel")
    jedis.hmset("db:tb:id值", map) //传map为了避免每调redis
    val value: String = jedis.hget("Db:Table:id", "name")
    println(value)
  }

}