package com.lock.login.detect

import com.lock.login.entry.{LoginEvent, LoginFailWarning}
import com.lock.login.function.LoginFailSelect
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * 解决：1.乱序数据问题
  *      2.报警不及时 ==》 2s内只要连续登入失败超过次数，即报警
  *
  * author  Lock.xia
  * Date 2021-01-06
  */
object LoginFailWithCEP {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[LoginEvent] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\LoginLog.csv")
      .map((k: String) => {
        val dataArr: Array[String] = k.split(",")
        LoginEvent(dataArr(0).toLong, dataArr(1).toString, dataArr(2).toString, dataArr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = {
          element.eventTime * 1000L
        }
      })


    // 定义 CEP 的 匹配模式 详细见官网，libraries[Event Processing(CEP)] 目录下
    val loginFailPattern:Pattern[LoginEvent,LoginEvent] = Pattern
      .begin[LoginEvent]("firstFail").where((_: LoginEvent).eventType == "fail") // 第一次登入失败  如果需要检测多次 ==》 .times() 默认是宽松紧邻 ；  .times().consecutive() 是严格紧邻
      .next("secondFail").where((_: LoginEvent).eventType == "fail") // 第二次登入失败  next() ===> 紧跟着 followedBy() ===> 不需要紧跟着
      .within(Time.seconds(2)) // 在2s内检测匹配


    // 在数据流中匹配出定义好的模式
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(dataStream.keyBy((_: LoginEvent).userId),loginFailPattern)

    val resultStream: DataStream[LoginFailWarning] = patternStream.select(new LoginFailSelect())

    resultStream.print("result")

    env.execute("login fail with CEP")

  }

}
