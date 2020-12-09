package com.lock.api

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
/**
  * author  Lock.xia
  * Date 2020-10-20
  */
object TransformTest {
  def main(args: Array[String]): Unit = {

    // 获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val inputDataDS: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_v1\\src\\main\\resources\\sensor.txt")

    val resultDS: DataStream[SensorReading] = inputDataDS.map { k: String =>
      val strArr: Array[String] = k.split(",")
      SensorReading(strArr(0), strArr(1).toLong, strArr(2).toDouble)
    }.keyBy(0).minBy("temperature") //.reduce(new MyReduce())
      //      .keyBy((d: SensorReading) => d.id)
      //      .keyBy(new MyIdSelector())
      //      .sum("timestamp")
//      .reduce((curRes, newData) =>
//      SensorReading(curRes.id, curRes.timestamp.max(newData.timestamp), curRes.temperature.min(newData.temperature)))
     // 聚合出每一个sensor的最大的时间戳和最小的温度值

    val splitStream: SplitStream[SensorReading] = inputDataDS.map { k: String =>
      val strArr: Array[String] = k.split(",")
      SensorReading(strArr(0), strArr(1).toLong, strArr(2).toDouble)
    }.split((data: SensorReading) => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })

    val highStream: DataStream[SensorReading] = splitStream.select("high")
    val lowStream: DataStream[SensorReading] = splitStream.select("low")
    val allStream: DataStream[SensorReading] = splitStream.select("low","high")

    val connectStream: ConnectedStreams[SensorReading, SensorReading] = highStream.connect(lowStream)
    connectStream.map (
      (d1: SensorReading) => d1.timestamp,
      (d2: SensorReading) => d2.id
    )

    highStream.print("highStream :")
    lowStream.print("lowStream :")
    allStream.print("allStream :")
//    resultDS.print("sensor :")



    env.execute("transform 算子")
  }

}


// 自定义函数类，key选择器

class MyIdSelector extends KeySelector[SensorReading,String]{
  override def getKey(in: SensorReading): String = in.id
}

class MyReduce extends ReduceFunction[SensorReading]{
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id,t.timestamp.max(t1.timestamp),t.temperature.max(t1.temperature))
  }
}