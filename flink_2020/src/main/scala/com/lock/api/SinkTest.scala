package com.lock.api

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.util.Collector
import org.elasticsearch.client.Requests


/**
  * author  Lock.xia
  * Date 2020-10-22
  */
object SinkTest {
  def main(args: Array[String]): Unit = {
    // 获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // 设置时间语义 事件时间 进程时间  执行时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputDataDS: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_v1\\src\\main\\resources\\sensor.txt")

    val resultDS: DataStream[SensorReading] = inputDataDS.map { k: String =>
      val strArr: Array[String] = k.split(",")
      SensorReading(strArr(0), strArr(1).toLong, strArr(2).toDouble)
    }.assignAscendingTimestamps(_.temperature.toInt * 1000L) // 设置时间戳


    // 窗口
    resultDS
      .keyBy(0).timeWindow(Time.seconds(15),Time.seconds(5)).apply(new MyWindowFun())
//      .window(EventTimeSessionWindows.withGap(Time.minutes(1)))
//        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
//        .timeWindow(Time.seconds(15))
//        .reduce(new MyReduce)





    // sink

    // 1-文件 csv 将被弃用 数据类型是 Tuple
    resultDS.writeAsCsv("E:\\Code\\bigdata\\flink_project_v1\\src\\main\\resources\\out.txt")

    // 2-addSink
    //resultDS.addSink(new StreamingFileSink[SensorReading]())


    // es
    val hosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]() {
      {
        add(new HttpHost("localhost", 9200))
      }
    }
    val esSinkFunction: ElasticsearchSinkFunction[SensorReading] = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        println("saving data: " + t)
        val json = new util.HashMap[String, String]()
        json.put("data", t.toString)
        val indexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(json)
        requestIndexer.add(indexRequest)
        println("saved successfully")
      }
    }

    resultDS.addSink(new ElasticsearchSink.Builder[SensorReading](hosts,esSinkFunction).build())


    // jdbc sink
    resultDS.addSink(new MyJdbcSink())


    env.execute("sink test ")

  }

}


// 自定义一个 sinkFunction
class MyJdbcSink() extends RichSinkFunction[SensorReading]{

  // 首先定义一个sql连接，以及预编译语句
  var conn:Connection=_
  var insertStmt: PreparedStatement =_
  var updateStmt: PreparedStatement =_

  // 在open生命周期方法中，创建连接以及预编译语句
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456")
    insertStmt = conn.prepareStatement("insert into temp (sensor temperature) values (?,?)")
    updateStmt = conn.prepareStatement("update into set sensor=? where temperature=?)")
  }
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // 执行更新语句
    updateStmt.setInt(1,value.id.toInt)
    updateStmt.setDouble(2,value.temperature)
    updateStmt.execute()

    // 如果没有数据就执行插入操作
    if(updateStmt.getUpdateCount == 0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }
  }
  // 关闭操作
  override def close(): Unit = {
    updateStmt.close()
    insertStmt.close()
    conn.close()
  }
}

// 定义一个全窗口函数
class MyWindowFun() extends WindowFunction[SensorReading,(Long,Int),Tuple,TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[(Long,Int)]): Unit = {
    out.collect((window.getStart,input.size))
  }
}