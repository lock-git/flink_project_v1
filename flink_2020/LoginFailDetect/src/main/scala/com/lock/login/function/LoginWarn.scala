package com.lock.login.function

import java.sql.Timestamp
import java.util

import com.lock.login.entry.{LoginEvent, LoginFailWarning}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * author  Lock.xia
  * Date 2021-01-05
  */
class LoginWarn(failTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {


  // 定义状态 登入失败信息 + 闹钟时间
  lazy val clockState: ValueState[Long] = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("time-clock", classOf[Long]))
  lazy val failState: ListState[LoginEvent] = getRuntimeContext.getListState[LoginEvent](new ListStateDescriptor[LoginEvent]("fail-info-list", classOf[LoginEvent]))


  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {

    if (value.eventType == "fail") {
      failState.add(value)
      if(clockState.value() == 0){
        val ts: Long = value.eventTime * 1000L + 2000L
        clockState.update(ts)
        ctx.timerService().registerEventTimeTimer(ts)
      }

    } else { // 登入成功
      failState.clear()
      clockState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {

    val failEvents: util.Iterator[LoginEvent] = failState.get().iterator()

    val events: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
    while (failEvents.hasNext) events += failEvents.next()

    if (events.size >= failTimes) {
      val userId: Long = ctx.getCurrentKey
      out.collect(LoginFailWarning(userId, new Timestamp(events.head.eventTime * 1000L).toString  , new Timestamp(events.last.eventTime * 1000L).toString,s"login fail ${events.size} times  in 2s"))
    }

    failState.clear()
    clockState.clear()
  }
}

