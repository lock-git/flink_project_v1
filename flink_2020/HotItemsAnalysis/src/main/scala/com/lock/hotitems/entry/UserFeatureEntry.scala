package com.lock.hotitems.entry

/**
  * author  Lock.xia
  * Date 2021-01-11
  */
object UserFeatureEntry {

}


case class DeviceLogEntry(
                           deviceId: String,
                           platform: String,
                           version: String,
                           channel: String,
                           ip: String,
                           eventid: String,
                           eventcontent: String,
                           areacode: String,
                           begintime: String
                         )