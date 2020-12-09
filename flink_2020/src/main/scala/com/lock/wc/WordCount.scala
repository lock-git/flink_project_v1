package com.lock.wc

import org.apache.flink.api.scala._

/**
  * author  Lock.xia
  * Date 2020-05-14
  *
  * 批处理
  */
object WordCount {
  def main(args: Array[String]): Unit = {


    // 创建一个 批处理 的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


    // 读取数据
    val inputDataSet: DataSet[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_v1\\src\\main\\resources\\word.txt")

    // 基于DataSet做转换，按照空格分词打散，然后按照word作为key做group by
    val resultDataSet: DataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0) // 以二元组中第一个元素作为key
      .sum(1)  // 以 聚合二元组中第二个元素的值


    // 打印输出
    resultDataSet.print()

  }
}
