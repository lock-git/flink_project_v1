package com.lock.day5

import com.lock.api.SensorReading
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.{FsStateBackend, FsStateBackendFactory}
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
  * author  Lock.xia
  * Date 2020-11-23
  */
object SideOutputTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStateBackend(new MemoryStateBackend())
    env.setStateBackend(new FsStateBackend(""))
    env.setStateBackend(new RocksDBStateBackend("",true))

    val inputStream: DataStreamSource[String] = env.socketTextStream("localhost", 7777)

    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })

    // 用 processFunction 的侧输出流实现分流操作
    val highTempStream = dataStream
      .process(new SplitTempProcessor(36.9))

    val lowTempStream = highTempStream.getSideOutput(new OutputTag[(String, Double, Long)]("lowTemp"))

    // 打印输出
    highTempStream.print("high")
    lowTempStream.print("low")

    env.execute("side out put ")
  }
}


// 自定义 processFunction ，用于输出高低温数据
class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {

    // 判断当前温度是否在大于 threshold[阈值]，大于就直接输出，小于则放入侧输出流
    if (value.temperature > threshold) {
      out.collect(value)
    } else {
      // 侧输出流 对比 split 很大的优势是，输出结果的格式是多样化的，根据自己需求来定义
      ctx.output(new OutputTag[(String, Double, Long)]("lowTemp"), (value.id, value.temperature, value.timestamp))
    }
  }
}