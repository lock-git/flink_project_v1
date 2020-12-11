package com.lock.tableapi

import java.util.Properties

import com.lock.api.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row

/**
  * author  Lock.xia
  * Date 2020-12-10
  */
object TableExample {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据创建 dataStream
    val inputStream: DataStream[String] = env.readTextFile("E:\\Code\\bigdata\\flink_project_v1\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })


    // 创建表执行环境
    val tab_env: StreamTableEnvironment = StreamTableEnvironment.create(env)


    // 基于流，转换为一张表，然后进行操作
    tab_env.connect(new FileSystem().path(""))
      .withFormat(new OldCsv())
      .withSchema(new Schema().field("id", DataTypes.STRING()))
      .createTemporaryTable("inputTable")

    // 连接kafka
    tab_env.connect(
      new Kafka()
        .version("0.11") // 定义kafka的版本
        .topic("sensor") // 定义主题
        .property("zookeeper.connect", "localhost:2181")
        .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    // table API 的调用
    val kafkaTable: Table = tab_env.from("kafkaInputTable")


    // 注意导包  import org.apache.flink.table.api.scala._
    kafkaTable.groupBy('id).select('id, 'id.count as 'ct)

    kafkaTable.select('id, 'timestamp).filter('id === "sensor_1")


    // SQL 的查询
    val resultTable: Table = tab_env.sqlQuery("select * from kafkaInputTable where id='sensor_1'")


    // 将 dataStream 转化为 表
    // 基于名称对应字段
    tab_env.fromDataStream(dataStream, 'id, 'timestamp as 'ts, 'templ)

    // 基于位置对应字段
    tab_env.fromDataStream(dataStream, 'f1, 'f2, 'f3)


    // 将 dataStream 转化为 临时视图
    tab_env.createTemporaryView("sensor_tmp_view", dataStream)
    tab_env.createTemporaryView("sensor_tmp_view", dataStream, 'id, 'timestaple, 'tmp)


    // 将结果输出
    // 输出到文件
    tab_env.connect(new FileSystem().path("fhsdkhf-----dfsadlkj"))
      .withFormat(new Csv())
      .withSchema(new Schema().field("id", DataTypes.STRING()).field("temp", DataTypes.DOUBLE()))
      .createTemporaryTable("outputTable")

    resultTable.insertInto("outputTable")


    // 输出到 kafka
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    tab_env.connect(new Kafka()
      .version("0.11")
      .topic("sinkTest")
      .properties(properties))
      .withFormat(new Csv())
      .withSchema(new Schema().field("id", DataTypes.STRING()).field("timestmp", DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaOutputTable")
    resultTable.insertInto("kafkaOutputTable")


    //  输出到 ES  ===》 ElasticSearch的connector可以在upsert（update+insert，更新插入）模式下操作
    tab_env.connect(new Elasticsearch().version("6").host("localhost", 9200, "http")
      .index("sensor").documentType("temp"))
      .inUpsertMode() // 指定Upsert模式
      .withFormat(new Json())
      .withSchema(new Schema().field("id", DataTypes.STRING()).field("ct", DataTypes.BIGINT()))
      .createTemporaryTable("esOutputTable")

    resultTable.insertInto("esOutputTable")

    // 输出到 mysql
    val sinkDDL :String  =
      """
        |create table jdbcOutputTable(
        | id varchar(20) not null,
        | cnt bigint not null)
        |with (
        |     'connector.type' = 'jdbc',
        |     'connector.url' = 'jdbc:mysql://localhost:3306/test',
        |     'connector.table' = 'sensor_count',
        |     'connector.driver' = 'com.mysql.jdbc.Driver',
        |     'connector.username' = 'root',
        |     'connector.password' = '123456'
        |     )
      """.stripMargin
    tab_env.sqlUpdate(sinkDDL)
    resultTable.insertInto("jdbcOutputTable")

    // 将结果表转化为 DataStream
    // 两种模式： Append Mode ===> 追加模式  Retract Mode ===> 撤回模式
     tab_env.toAppendStream[Row](resultTable)

    tab_env.toRetractStream[(String,Long)](resultTable)







    env.execute("table example job")
  }

}
