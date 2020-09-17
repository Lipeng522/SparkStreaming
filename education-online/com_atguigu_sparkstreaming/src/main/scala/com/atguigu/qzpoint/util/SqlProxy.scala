package com.atguigu.qzpoint.util

import java.sql.{Connection, PreparedStatement, ResultSet}

trait QueryCallback {
  def process(rs: ResultSet)
}

class SqlProxy {
  private var rs: ResultSet = _
  private var psmt: PreparedStatement = _

  /**
    * 执行修改语句
    * 不能用select * 因为他返回值是一个结果集 ， 返回值是int类型接收的
    * @param conn
    * @param sql
    * @param params 占位符
    * @return
    */
  def executeUpdate(conn: Connection, sql: String, params: Array[Any]): Int = {
    var rtn = 0
    try {
      //预执行
      psmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          psmt.setObject(i + 1, params(i))
        }
      }
      //注意！这里不是递归
      rtn = psmt.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  /**
    * 执行查询语句
    * 执行查询语句
    *
    * @param conn
    * @param sql
    * @param params
    * @return
    */
  def executeQuery(conn: Connection, sql: String, params: Array[Any], queryCallback: QueryCallback) = {
    rs = null
    try {
      psmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          psmt.setObject(i + 1, params(i))
        }
      }
      rs = psmt.executeQuery()
      //调了特质里的抽象方法，所以在用这个方法的时候要写一个匿名实现类
      queryCallback.process(rs)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def shutdown(conn: Connection): Unit = DataSourceUtil.closeResource(rs, psmt, conn)
}
