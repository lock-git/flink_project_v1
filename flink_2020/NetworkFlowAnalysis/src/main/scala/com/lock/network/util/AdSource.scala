package com.lock.network.util

import java.util.UUID

import com.lock.network.entry.MarketingUserBehavior
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

/**
  *
  * 自定义一个测试数据源
  *
  * author  Lock.xia
  * Date 2021-01-04
  */
class AdSource extends RichParallelSourceFunction[MarketingUserBehavior] {

  var running = true
  var channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")
  var behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")


  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {

    val maxElements: Long = Long.MaxValue
    var count: Long = 0L

    while (running && count < maxElements) {
      val id: String = UUID.randomUUID().toString
      val behavior: String = behaviorTypes(Random.nextInt(behaviorTypes.size))
      val channel: String = channelSet(Random.nextInt(channelSet.size))
      val ts: Long = System.currentTimeMillis()


      ctx.collectWithTimestamp(MarketingUserBehavior(id, behavior, channel, ts), ts)
      count += 1
      Thread.sleep(50L)

    }
  }

  override def cancel(): Unit = running = false
}
