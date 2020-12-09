package com.lock.api

import java.util.Random

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.immutable

/**
  * author  Lock.xia
  * Date 2020-10-19
  */
class MySensorSource extends SourceFunction[SensorReading]{

  // flag: 表示数据源是否还在正常运行
  var running: Boolean = true

  override def cancel(): Unit = {
    running = false
  }

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 初始化一个随机数发生器
    val rand = new Random()

    var curTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(
      (i: Int) => ( "sensor_" + i, 65 + rand.nextGaussian() * 20 ) // 高斯随机数，-1~1[正态分布]
    )

    while(running){
      // 更新温度值
      curTemp = curTemp.map(
        (t: (String, Double)) => (t._1, t._2 + rand.nextGaussian() ) // 高斯随机数，-1~1[正态分布]
      )
      // 获取当前时间戳
      val curTime = System.currentTimeMillis()

      curTemp.foreach(
        (t: (String, Double)) => ctx.collect(SensorReading(t._1, curTime, t._2))
      )
      Thread.sleep(100)
    }
  }
}
