package com.lock.api

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * author  Lock.xia
  * Date 2020-11-18
  */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {

    // 获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // checkpoint 相关配置

    // 启用检查点，指定触发检查点间隔时间[毫秒]
    env.enableCheckpointing(1000L)

    // 其他配置
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(30 * 1000L)  // 设置超时时间
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2) // 设置最大同时处理的checkpoint的个数
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500) // 两次checkpoint之间至少留多少时间去处理数据
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true) // true -> 发生故障从checkpoint恢复，false -> 从自定义的savepoint恢复
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3) // 允许的最大失败次数


    // 重启策略配置
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000L))
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5,Time.of(5,TimeUnit.MINUTES),Time.of(10,TimeUnit.SECONDS)))


    val streamDS: DataStream[String] = env.socketTextStream("localhost", 7777)
    streamDS.map { k: String =>
      val strArr: Array[String] = k.split(",")
      SensorReading(strArr(0), strArr(1).toLong, strArr(2).toDouble)
    }.keyBy("id").process(new TempInver(10 * 1000L))

  }
}


// 自定义 richMapFunction


/**
  * 需求 ：监测温度，如果连续10s内温度都在上升，则报警
  *
  * 自定义 keyedProcessFunction
  *
  * @param intervarl
  */
class TempInver(intervarl: Long) extends KeyedProcessFunction[Tuple, SensorReading, String] {

  // 懒加载
  // 由于需要跟之前的温度值做对比，所以将上一个温度保存为状态

  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  // 为了方便删除定时器，还需要保存定时器的时间戳
  lazy val curTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-timer-ts", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {

    // 取出状态
    val lastTemp: Double = lastTempState.value()
    val curTimesTs: Long = curTimerState.value()

    // 将上次的温度值更新当前数据的温度值
    lastTempState.update(value.temperature)

    // 逻辑判断
    // 判断当前温度值，如果比之前的温度高，并且没有定时器的话，注册10s后的定时器

    if (value.temperature > lastTemp && curTimesTs == 0) {
      val ts: Long = ctx.timerService().currentProcessingTime() + intervarl
      ctx.timerService().registerProcessingTimeTimer(ts)
      curTimerState.update(ts)
    } else if (value.temperature <= lastTemp) { // 如果温度下降，删除定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimesTs)
      // 删除定时器后，清空状态
      curTimerState.clear()
    }

  }

  // 若定时器触发，温度在10s内持续上升，报警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

    out.collect("温度值连续 " + intervarl / 1000 + " 秒上升")
    // 报警后，清空定时器状态
    curTimerState.clear()

  }
}
