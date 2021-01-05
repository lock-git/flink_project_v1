package com.lock.network.function

import com.lock.network.entry.{AdClickLog, BlackListWarning}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * author  Lock.xia
  * Date 2021-01-04
  */
class AdBlackListFilter(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {

  // 定义三类状态  计数值-状态值-闹钟时间
  lazy val countValueState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("user_ad_count_state", classOf[Long]))
  lazy val tsValueState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("user_ad_ts_state", classOf[Long]))
  lazy val blackState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("black_flag", classOf[Boolean]))

  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {

    val count: Long = countValueState.value()

    // 如果用户第一次点击某个广告，则注册闹钟
    if (count == 0) {

      // 第二天凌晨的时间戳【这里没有考虑北京时间和伦敦时间，实际开发需要考虑 8 小时的时差】
      val ts: Long = ((ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24)) + 1) * (1000 * 60 * 60 * 24)
      tsValueState.update(ts)

      // 注意定时器的方式为processingTime时，保证到了凌晨定时清理。如果为eventTime，到了凌晨一直没有来数据，则状态一直保存，不合理
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    if (count > maxCount) {

      val black_flag: Boolean = blackState.value()

      if (!black_flag) { // 大于最大点击次数，但是还没加入黑名单的用户
        blackState.update(true)
        ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "click times over " + maxCount + " today."))
      }

      // 对于超过最大次数的点击不再计入下一步的计算，相当于过滤掉了

    } else {
      countValueState.update(count + 1)
      out.collect(value)
    }

  }


  // 到点清空状态
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {

    if (tsValueState.value() == timestamp) {
      countValueState.clear()
      tsValueState.clear()
      blackState.clear()

    }
  }
}
