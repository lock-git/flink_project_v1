package com.lock.network.util

import java.sql.{Connection, DriverManager, Statement}

/**
  *
  * 连接数据库
  */
object MysqlUtils extends App {

  val MYSQL_CONNECTION_URL_PROD = "jdbc:mysql://123.34.222.87:3306/flink?useUnicode=true&characterEncoding=UTF-8&useSSL=false&useTimezone=true&serverTimezone=Asia/Shanghai"
  val M_U_CHN = "root"
  val M_P_CHN = "123456"

  val M_DRIVER_NEW = "com.mysql.cj.jdbc.Driver"


  //获取Mysql连接Map
  def getFlinkMysqlMap = {
    val properties = Map(
      "driver" -> "com.mysql.cj.jdbc.Driver",
      "url" -> "jdbc:mysql://172.30.201.57:3306/flink?useUnicode=true&characterEncoding=UTF-8&useSSL=false&useTimezone=true&serverTimezone=Asia/Shanghai",
      "user" -> "root",
      "password" -> "123456"
    )
    properties
  }

  //获取Mysql连接通道
  def getFlink2MysqlConnection: Connection = {
    var connection: Connection = null
    try {
      connection = DriverManager.getConnection(MYSQL_CONNECTION_URL_PROD, M_U_CHN, M_P_CHN)
    } catch {
      case e: Exception =>
        println("Mysql Connection Exception")
        println(e)
    }
    connection
  }

  // 删除操作
  def deleteTable(deleteSQL: String): Unit = {
    // 获取连接
    Class.forName(M_DRIVER_NEW)
    val connection: Connection = DriverManager.getConnection(MYSQL_CONNECTION_URL_PROD, M_U_CHN, M_P_CHN)
    val statement: Statement = connection.createStatement
    try {
      statement.executeUpdate(deleteSQL)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    //关闭连接，释放资源
    connection.close
  }

  // 删除操作
  def deleteMotoWriteCtrTable(deleteSQL: String): Unit = {
    // 获取连接
    Class.forName(M_DRIVER_NEW)
    val connection: Connection = DriverManager.getConnection(MYSQL_CONNECTION_URL_PROD, M_U_CHN, M_P_CHN)
    val statement: Statement = connection.createStatement
    try {
      statement.executeUpdate(deleteSQL)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    //关闭连接，释放资源
    connection.close()
  }

  // 删除操作
  def deletejddPortraitTable(deleteSQL: String): Unit = {
    // 获取连接
    Class.forName(M_DRIVER_NEW)
    val connection: Connection = DriverManager.getConnection(MYSQL_CONNECTION_URL_PROD, M_U_CHN, M_P_CHN)
    val statement: Statement = connection.createStatement
    try {
      statement.executeUpdate(deleteSQL)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    //关闭连接，释放资源
    connection.close()
  }
}
