package com.lock.network.function

import com.lock.network.entry.UserBehavior
import org.apache.flink.api.common.functions.MapFunction

import scala.util.Random

/**
  * author  Lock.xia
  * Date 2020-12-30
  */
class UvMapper() extends MapFunction[UserBehavior, (String, Long)] {
  override def map(value: UserBehavior): (String, Long) = {
    val str: String = Random.nextString(10)
    (str, value.userId)
  }
}
