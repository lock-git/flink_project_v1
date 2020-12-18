package com.lock.tableapi

import java.util.Properties

import com.lock.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row

/**
  * author  Lock.xia
  * Date 2020-12-14
  */
object TimeAndWindow {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 使用eventTime 必须指定时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    // 读取数据创建 dataStream
    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_v1\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream.map((data: String) => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) { // 设置最大的乱序程度
        override def extractTimestamp(element: SensorReading) = element.timestamp * 1000L
      }) // 设置watermarks


    // 创建表执行环境
    val tab_env: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 定义 processing time [正常这么使用 ，其他方式，可能外部系统不支持]
    val sensoe_tab_process: Table = tab_env.fromDataStream(dataStream, 'id, 'timestamp as 'ts, 'templ, 'pt.proctime)


    // 定义 event time [正常这么使用 ，其他方式，可能外部系统不支持]
    val sensoe_tab_event: Table = tab_env.fromDataStream(dataStream, 'id, 'timestamp as 'ts, 'templ, 'pt.rowtime)

    sensoe_tab_process.printSchema()
    sensoe_tab_process.toAppendStream[Row].print()

    sensoe_tab_event.printSchema()
    sensoe_tab_event.toAppendStream[Row].print()


    env.execute("table window time !")
  }

}
