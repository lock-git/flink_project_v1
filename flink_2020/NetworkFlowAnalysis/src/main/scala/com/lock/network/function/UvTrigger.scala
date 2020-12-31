package com.lock.network.function

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  *
  * 定义一个触发器，没来一条数据触发一次窗口，并清空窗口数据
  *
  * author  Lock.xia
  * Date 2020-12-30
  */


class UvTrigger() extends Trigger[(String, Long), TimeWindow] {
  // 每来一条数据，出发一次窗口操作，并清空窗口
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}
