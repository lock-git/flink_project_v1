package com.lock.hotitems.tableSql

import java.sql.Timestamp

import com.lock.hotitems.entry.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

/**
  * author  Lock.xia
  * Date 2021-01-14
  */
object HotItemsTopWithSQL {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream.map((k: String) => {
      val dataArr: Array[String] = k.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toLong, dataArr(3), dataArr(4).toLong)
    }).assignAscendingTimestamps((_: UserBehavior).timestamp * 1000L)

    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    tableEnv.createTemporaryView("item_stream_table_tmp", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    val resultSQL: String =
      """
        |SELECT itemId,window_end,cnt,rk FROM (
        |   SELECT itemId,
        |          cnt,
        |          window_end,
        |          ROW_NUMBER() OVER(PARTITION BY window_end ORDER BY cnt DESC) AS rk
        |   FROM (
        |       SELECT itemId,
        |                  COUNT(itemId) AS cnt,
        |              HOP_END(ts , INTERVAL '5' MINUTE, INTERVAL '1' HOUR) AS window_end
        |       FROM item_stream_table_tmp
        |       WHERE behavior='pv'
        |       GROUP BY HOP(ts , INTERVAL '5' MINUTE, INTERVAL '1' HOUR),itemId
        |   ) AS m
        |) AS n
        |WHERE rk <= 5
      """.stripMargin


    val resultStream: DataStream[(Boolean, (Long, Timestamp, Long, Long))] = tableEnv.sqlQuery(resultSQL).toRetractStream[(Long, Timestamp, Long, Long)]

    resultStream.print("result")

    env.execute("hot item with SQL")

  }

}
