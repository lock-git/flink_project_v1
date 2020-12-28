package com.lock.network.function

import com.lock.network.entry.PvCount
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
  * author  Lock.xia
  * Date 2020-12-28
  */
class PagePvProcess() extends KeyedProcessFunction[Long, PvCount, PvCount] {

  lazy val countValueState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("pv-count", classOf[Long]))

  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {

    countValueState.update(countValueState.value() + value.count)

    ctx.timerService().registerEventTimeTimer(value.WindowEnd)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {

    out.collect(PvCount(timestamp, countValueState.value()))
    countValueState.clear()
  }
}
