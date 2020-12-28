package com.lock.network.function

import com.lock.network.entry.UserBehavior
import org.apache.flink.api.common.functions.MapFunction

import scala.util.Random

/**
  * author  Lock.xia
  * Date 2020-12-28
  */
class PvMap() extends MapFunction[UserBehavior, (String, Long)] {
  override def map(value: UserBehavior): (String, Long) = {
    val randomStr: String = Random.nextInt(8).toString
    (randomStr, 1L)
  }
}
