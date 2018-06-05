package util

import module.Context
import java.io.BufferedInputStream
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL

class Requestor {

  @throws[Exception]
  def getURLPage(targetUrl: TargetUrl, context: Context): String = {
    var sb: StringBuffer = null
    val url: URL = new URL(targetUrl.getTarget)
    sb = new StringBuffer("")
    val con: HttpURLConnection = url.openConnection.asInstanceOf[HttpURLConnection]
    HttpURLConnection.setFollowRedirects(true)
    con.setInstanceFollowRedirects(false)
    con.setReadTimeout(10000)
    con.setConnectTimeout(5000)
    con.connect()
    val br: BufferedReader = new BufferedReader(new InputStreamReader(con.getInputStream, if (context.getEncoding != null) context.getEncoding
    else "UTF-8"))
    var s: String = br.readLine
    while (s != null) {
      sb.append(s + "\n")
      s = br.readLine
    }
    con.disconnect()
    sb.toString
  }

  @throws[Exception]
  def getPageWithStatus(targetUrl: TargetUrl, context: Context, timeAndSize: Array[Long]): String = {
    var sb: StringBuffer = null
    val url: URL = new URL(targetUrl.getTarget)
    sb = new StringBuffer("")
    val con: HttpURLConnection = url.openConnection.asInstanceOf[HttpURLConnection]
    HttpURLConnection.setFollowRedirects(true)
    con.setInstanceFollowRedirects(false)
    con.setReadTimeout(10000)
    con.setConnectTimeout(5000)
    val start: Long = System.currentTimeMillis
    con.connect()
    val br: BufferedReader = new BufferedReader(new InputStreamReader(con.getInputStream, if (context.getEncoding != null) context.getEncoding
    else "UTF-8"))
    var s: String = br.readLine
    while (s != null) {
      sb.append(s + "\n")
      s = br.readLine
    }
    con.disconnect()
    timeAndSize(0) = System.currentTimeMillis - start
    timeAndSize(1) = sb.toString.length
    sb.toString
  }
}
