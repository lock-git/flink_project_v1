package com.lock.login.entry

/**
  * author  Lock.xia
  * Date 2021-01-05
  */
class LoginModel {

}

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

case class LoginFailWarning(userId: Long, startTime: String, endTime: String, warnInfo: String)