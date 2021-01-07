package com.lock.login.function

import java.util

import com.lock.login.entry.{LoginEvent, LoginFailWarning}
import org.apache.flink.cep.PatternSelectFunction

/**
  *
  * 将检测到的连续登入失败时间，包装成报警信息输出
  *
  * author  Lock.xia
  * Date 2021-01-07
  */
class LoginFailSelect() extends PatternSelectFunction[LoginEvent,LoginFailWarning]{

  // 匹配模式中的数据被定义成一个Map，key为事件名称  "firstFail"，"secondFail"
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {

    val firstFail: LoginEvent = map.get("firstFail").get(0)  // 没有循环次数，只有一条数据
    val secondFail: LoginEvent = map.get("secondFail").get(0)

    LoginFailWarning(firstFail.userId,firstFail.eventTime.toString,secondFail.eventTime.toString,"login fail 2 times!")
  }
}
