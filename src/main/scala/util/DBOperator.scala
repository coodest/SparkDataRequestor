package util

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

import com.mysql.jdbc.PreparedStatement
import module.Context

class DBOperator(val databaseName: String, val tableName: String, val refresh: Boolean, val context: Context) {
  private var con: Connection = null
  private var stat: Statement = null
  private val res: ResultSet = null
  private var pstmt: PreparedStatement = null
  try {
    Class.forName("com.mysql.jdbc.Driver")
    con = DriverManager.getConnection(context.getDatabaseAddress, "root", "root")
    stat = con.createStatement
    databaseSet(databaseName)
    if (refresh) {
      tableRefresh(tableName)
    }
  }
  catch {
    case e: SQLException => {
      e.printStackTrace()
    }
    case e: ClassNotFoundException => {
      e.printStackTrace()
    }
  }

  def databaseSet(databaseName: String) {
    try {
      var sql: String = "CREATE DATABASE IF NOT EXISTS " + databaseName
      pstmt = con.prepareStatement(sql).asInstanceOf[PreparedStatement]
      pstmt.executeUpdate
      sql = "USE " + databaseName
      pstmt = con.prepareStatement(sql).asInstanceOf[PreparedStatement]
      pstmt.executeUpdate
    }
    catch {
      case e: SQLException => {
        e.printStackTrace()
      }
    }
  }

  def tableRefresh(tableName: String) {
    try {
      var sql: String = "DROP TABLE IF EXISTS " + tableName
      pstmt = con.prepareStatement(sql).asInstanceOf[PreparedStatement]
      pstmt.executeUpdate

      sql = "CREATE TABLE `" + tableName + "` (`id` nvarchar (60),`content` nvarchar (200000), primary key (id));"
      pstmt = con.prepareStatement(sql).asInstanceOf[PreparedStatement]
      pstmt.executeUpdate

      sql = "DROP TABLE IF EXISTS " + context.CHECKEDENTITY
      pstmt = con.prepareStatement(sql).asInstanceOf[PreparedStatement]
      pstmt.executeUpdate

      sql = "CREATE TABLE `" + context.CHECKEDENTITY + "` (`id` nvarchar (60),primary key (id));"
      pstmt = con.prepareStatement(sql).asInstanceOf[PreparedStatement]
      pstmt.executeUpdate

      sql = "DROP TABLE IF EXISTS " + context.ENTITY
      pstmt = con.prepareStatement(sql).asInstanceOf[PreparedStatement]
      pstmt.executeUpdate

      sql = "CREATE TABLE `" + context.ENTITY + "` (`id` nvarchar (60),primary key (id));"
      pstmt = con.prepareStatement(sql).asInstanceOf[PreparedStatement]
      pstmt.executeUpdate

      sql = "DROP TABLE IF EXISTS " + context.CHECKING
      pstmt = con.prepareStatement(sql).asInstanceOf[PreparedStatement]
      pstmt.executeUpdate

      sql = "CREATE TABLE `" + context.CHECKING + "` (`id` nvarchar (60),primary key (id));"
      pstmt = con.prepareStatement(sql).asInstanceOf[PreparedStatement]
      pstmt.executeUpdate

      sql = "DROP TABLE IF EXISTS " + context.BBQueue
      pstmt = con.prepareStatement(sql).asInstanceOf[PreparedStatement]
      pstmt.executeUpdate

      sql = "CREATE TABLE `" + context.BBQueue + "` (`id` nvarchar (60), `value` double, primary key (id));"
      pstmt = con.prepareStatement(sql).asInstanceOf[PreparedStatement]
      pstmt.executeUpdate
    }
    catch {
      case e: SQLException => {
        e.printStackTrace()
      }
    }
  }

  def addData(entity: String, content: String, time: String) {
    var sql: String = "INSERT INTO " + context.getTableName + "(id,content,time) VALUES"
    sql += "(\"" + entity + "\",\"" + content + "\",\"" + time + "\")"
    try {
      stat.executeUpdate(sql)
    }
    catch {
      case e: SQLException => {
        e.printStackTrace()
      }
    }
  }

  def addData(entity: String, contents: String) {
    val content = contents.replaceAll("\"", "'")
    var sql: String = "INSERT INTO " + context.getTableName + "(id,content) VALUES"
    sql += "(\"" + entity + "\",\"" + content + "\"" + ")"
    try {
      stat.executeUpdate(sql)
    }
    catch {
      case e: SQLException => {
        e.printStackTrace()
      }
    }
  }

  def addEntityTo(tableName: String, targetEntity: String) {
    var sql: String = "INSERT INTO " + tableName + "(id) VALUES"
    sql += "(\"" + targetEntity + "\")"
    try {
      stat.executeUpdate(sql)
    }
    catch {
      case e: SQLException => {
        e.printStackTrace()
      }
    }
  }

  def getTableSize(tableName: String): Int = {
    val sql: String = "SELECT * FROM " + tableName
    var rows: Int = 0
    try {
      val rs: ResultSet = stat.executeQuery(sql)
      rs.last
      rows = rs.getRow
    }
    catch {
      case e: SQLException => {
        e.printStackTrace()
      }
    }
    rows
  }

  def searchEntityIn(tableName: String, targetEntity: String): Array[String] = {
    val sql: String = "SELECT * FROM " + tableName + " WHERE id=\"" + targetEntity + "\""
    var content: Array[String] = null
    try {
      val rs: ResultSet = stat.executeQuery(sql)
      if (rs.next) {
        content = new Array[String](rs.getMetaData.getColumnCount)
        var i: Int = 0
        while (i < rs.getMetaData.getColumnCount) {
            content(i) = rs.getString(i + 1)
            i += 1
        }
      }
    }
    catch {
      case e: SQLException => {
        e.printStackTrace
      }
    }
    return content
  }

  def getEntityFrom(tableName: String, index: Int): String = {
    var place = index
    val sql: String = "SELECT * FROM " + tableName
    var entity: String = null
    try {
      val rs: ResultSet = stat.executeQuery(sql)
      if (!rs.next) {
        return null
      }
      while (place >= 1) {
        rs.next
        place -= 1
      }
      if (rs != null) {
        entity = rs.getString(1)
      }
    }
    catch {
      case e: SQLException => {
        e.printStackTrace()
      }
    }
    return entity
  }

  def entityContainsIn(tableName: String, targetEntity: String): Boolean = {
    val sql: String = "SELECT * FROM " + tableName + " WHERE id=\"" + targetEntity + "\""
    try {
      val rs: ResultSet = stat.executeQuery(sql)
      if (rs.next) {
        return true
      }
    }
    catch {
      case e: SQLException => {
        e.printStackTrace()
      }
    }
    return false
  }

  def delEntityFrom(tableName: String, targetEntity: String) {
    val sql: String = "DELETE FROM " + tableName + " WHERE id=\"" + targetEntity + "\""
    try {
      stat.executeUpdate(sql)
    }
    catch {
      case e: SQLException => {
        e.printStackTrace()
      }
    }
  }

  def pushQueue(targetRange: String, value: Double) {
    var sql: String = "INSERT INTO " + context.BBQueue + "(id, value) VALUES"
    sql += "(\"" + targetRange + "\", " + value + ")"
    try {
      stat.executeUpdate(sql)
    }
    catch {
      case e: SQLException => {
        e.printStackTrace
      }
    }
  }

  def topQueue: String = {
    val sql: String = "SELECT * FROM  " + context.BBQueue + " ORDER BY VALUE DESC LIMIT 1"
    var targetRange: String = ""
    try {
      val rs: ResultSet = stat.executeQuery(sql)
      if (rs.next) {
        targetRange = rs.getString(1)
      }
    }
    catch {
      case e: SQLException => {
        e.printStackTrace
      }
    }
    return targetRange
  }

  def pullQueue(targetRange: String) {
    val sql: String = "DELETE FROM " + context.BBQueue + " WHERE id=\"" + targetRange + "\""
    try {
      stat.executeUpdate(sql)
    }
    catch {
      case e: SQLException => {
        e.printStackTrace
      }
    }
  }

  def close() {
    try {
      if (res != null) {
        res.close()
      }
      if (stat != null) {
        stat.close()
      }
      if (con != null) {
        con.close()
      }
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
}
