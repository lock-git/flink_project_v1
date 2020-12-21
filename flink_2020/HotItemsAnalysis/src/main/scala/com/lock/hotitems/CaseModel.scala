package com.lock.hotitems

/**
  * 定义输入数据的样例类
  *
  * author  Lock.xia
  * Date 2020-12-18
  */
object CaseModel {

}

case class UserBehavior(userId: Long, itemId: Long, behavior: String, timestamp: Long)

case class ItemViewCount(ItemId: Long, WindowEnd: Long, count: Long)