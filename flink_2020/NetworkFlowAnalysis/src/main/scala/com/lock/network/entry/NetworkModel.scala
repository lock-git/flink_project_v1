package com.lock.network.entry

/**
  * author  Lock.xia
  * Date 2020-12-24
  */
object NetworkModel {

}

case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class PageViewCount(url: String, windowEnd: Long, count: Long)

case class UserBehavior(userId: Long, itemId: Long, categoryId: Long, behavior: String, timestamp: Long)

case class PvCount(WindowEnd: Long, count: Long)

case class UvCount(WindowEnd: Long, count: Long)

case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

case class MarketingCountView(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class CountByProvince(windowEnd: String, province: String, count: Long)

case class BlackListWarning(userId: Long, adId: Long, msg: String)