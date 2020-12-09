package com.lock.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * author  Lock.xia
  * Date 2020-05-14
  *
  * 流处理
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {

    // 创建 流处理的 执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置并行度
    env.setParallelism(9)

    // 接受 socket 文本流 ==》 用一个 nc  命令 像一个端口发送文件 [ nc -l -p 7777 ]
    //val inputStream: DataStream[String] = env.socketTextStream("localhost",7777)

    // 可以从程序运行的参数中读取 host 和 port
    val parames: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = parames.get("host")
    val port: Int = parames.getInt("port")
    val inputStream: DataStream[String] = env.socketTextStream(host,port)

    // 定义转化操作
    val resultStream: DataStream[(String, Int)] = inputStream.flatMap((_: String).split(" "))
      .filter((_: String).nonEmpty)
      .map(((_: String), 1)) // 每一步可以单独设置并行度,代码中设置的并行度的优先级最高
      .keyBy(0) // 按照第一个元素分组
      .sum(1).setParallelism(6) // 按照第二个元素聚合

    // 打印输出
    resultStream.print().setParallelism(1)

    // 启动
    env.execute("stream word count")

  }
}
