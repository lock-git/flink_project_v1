package com.lock.hotitems.tableSql

import java.sql.Timestamp

import com.lock.hotitems.entry.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}

/**
  * author  Lock.xia
  * Date 2021-01-14
  */
object HotItemsTopWithTable {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_git\\flink_2020\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream.map((k: String) => {
      val dataArr: Array[String] = k.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
    }).assignAscendingTimestamps((_: UserBehavior).timestamp * 1000L)

    // 要配置 Table Api ，配置表执行环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 将 dataStream 转化为 table , 提取需要的字段进行处理
    val dataTable: Table = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    // 分组 开窗 增量聚合
    // 注意：  'sw.end 获取得到的是 TimeStamp 类型，不是Long类型的时间戳
    val aggTable: Table = dataTable
      .where('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw) // 滑动窗口
      .groupBy('itemId, 'sw) // 分组聚合，必须包含窗口
      .select('itemId, 'itemId.count as 'cnt, 'sw.end as 'windowEnd)

    // 开窗函数 [ table api 暂时没有开窗函数， 使用 SQL 实现  ==> item_agg_table  ]
    tableEnv.createTemporaryView("item_agg_table", aggTable, 'itemId, 'cnt, 'windowEnd)
    val resultSql: String =
      """
        |SELECT itemId,windowEnd,cnt,rk FROM (
        |     SELECT itemId,cnt,windowEnd,
        |            ROW_NUMBER() OVER(PARTITION BY windowEnd ORDER BY cnt DESC) as rk
        |     FROM item_agg_table
        |) AS m
        |WHERE rk <= 5
      """.stripMargin

    // retract 模式 : 不断更新，一条false[舍弃旧的结果]一条true[更新结果]
    val resultStream: DataStream[(Boolean, (Long, Timestamp, Long, Long))] = tableEnv
      .sqlQuery(resultSql)
      .toRetractStream[(Long, Timestamp, Long, Long)]

    resultStream.print("result")

    env.execute("hot item with table api & sql")

  }

}
