package module.downloader

import module.Context
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.select.Elements
import java.io.FileNotFoundException
import java.util.concurrent.atomic.AtomicInteger

import util.{SequenceGenerator, Splitter, TargetUrl}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

class MCSDownloader(var context: Context) extends Downloader with Runnable {
  private var weight: AtomicInteger = null
  val break = new Breaks

  override def reset {
    val start: Int = 1
    val end: Int = 10
    var i: Long = start
    while (i <= end) {
      context.getDbOperator.addEntityTo(context.ENTITY, "Q" + String.valueOf(i))

      i += 1
    }
    context.getDbOperator.pushQueue(String.valueOf(start) + Splitter.TINY + String.valueOf(end), 1)
  }

  override def download {
    val devideFork: Int = 2
    val sampleRatio: Double = 0.01
    weight = new AtomicInteger
    break.breakable(
      do {
        val targetRange: String = context.getDbOperator.topQueue
        if (targetRange ne "") {
          val rangeStr: Array[String] = targetRange.split(Splitter.TINY)
          val range: Array[Integer] = new Array[Integer](2)
          range(0) = rangeStr(0).toInt
          range(1) = rangeStr(1).toInt
          val segments: ListBuffer[Array[Integer]] = divide(range, devideFork)
          for (segment <- segments) {
            if (segment(0) - segment(1) == 0) {
              sampleMiu(segment, sampleRatio)
            }
            else {
              val segRangeStr: Array[String] = new Array[String](2)
              segRangeStr(0) = segment(0).toString
              segRangeStr(1) = segment(1).toString
              context.getDbOperator.pushQueue(segRangeStr(0) + Splitter.TINY + segRangeStr(1), sampleMiu(segment, sampleRatio))
            }
          }
          context.getDbOperator.pullQueue(targetRange)
        }
        else {
          break.break()
        }
      } while (true)
    )
  }

  private def divide(range: Array[Integer], shares: Int): ListBuffer[Array[Integer]] = {
    val segments: ListBuffer[Array[Integer]] = new ListBuffer[Array[Integer]]
    val interval: Int = Math.max(Math.abs(range(1) - range(0) + 1) / shares, 1)
    var pivot: Int = range(0)
    while (pivot <= range(1)) {
      {
        val temp: Array[Integer] = new Array[Integer](2)
        temp(0) = pivot
        if ((pivot + interval - 1) <= range(1)) {
          temp(1) = pivot + interval - 1
        }
        else {
          temp(1) = range(1)
        }
        segments += temp
        pivot += interval
      }
    }
    return segments
  }

  private def sampleMiu(segment: Array[Integer], sampleRatio: Double): Double = {
    val sampleNum: Int = Math.max(((segment(1) - segment(0)) * sampleRatio).toInt, 1)
    val sg:SequenceGenerator = new SequenceGenerator
    val sampleSequence: Array[Int] = sg.uniformRandSeq(segment(0), segment(1), sampleNum)
    weight.set(0)
    var i: Int = 0
    while (i < sampleSequence.length) {
      val content: Array[String] = context.getDbOperator.searchEntityIn(context.getTableName, "Q" + sampleSequence(i))
      if (content != null) {
        val property: Array[String] = content(1).split(Splitter.HEAVY)(2).split(Splitter.MEDIUM)
        var j: Int = 0
        while (j < property.length) {
          val links: Array[String] = property(j).split(Splitter.LIGHT)
          val link: Array[String] = links(1).split(Splitter.TINY)
          var k: Int = 0
          while (k < link.length) {
            weight.incrementAndGet

            k += 1
          }

          j += 1
        }
      }
      else {
        makeJob("Q" + sampleSequence(i))
      }

      i += 1
    }
    try {
      if (context.getExecutor.getPoolSize > 0) {
        break.breakable(
          while (true) {
            Thread.sleep(100)
            val open: Int = context.getNThread.get
            if (open == 0) {
              break.break
            }
        })
      }
    }
    catch {
      case e: InterruptedException => {
        e.printStackTrace
      }
    }
    val volume: Int = segment(1) - segment(0) + 1
    val miu: Double = (weight.get).toDouble / volume.toDouble
    return miu
  }

  def makeJob(entity: String) {
    context.getDbOperator.delEntityFrom(context.ENTITY, entity)
    context.getDbOperator.addEntityTo(context.CHECKING, entity)
    val targetUrl: TargetUrl = new TargetUrl(context.getWebEntityURLHead + entity)
    targetUrl.setRequestTime(context.getRequests)
    context.getIndex.push(targetUrl)
    context.getNThread.incrementAndGet
    context.getExecutor.execute(this)
  }

  def run {
    val targetUrl: TargetUrl = context.getIndex.pop
    var temp: Array[String] = targetUrl.getTarget.split("/")
    val entity: String = temp(temp.length - 1)
    try {
      val html: String = context.getRequestor.getURLPage(targetUrl, context)
      val doc: Document = Jsoup.parse(html)
      val label: Elements = doc.getElementsByClass("wikibase-title-label")
      val description: Elements = doc.getElementsByClass("wikibase-descriptionview-text")
      var content: String = ""
      content += label.get(0).text + Splitter.HEAVY
      content += description.get(0).text + Splitter.HEAVY
      val property: Elements = doc.getElementsByClass("wikibase-statementgroupview-property-label")
      var i: Int = 0
      while (i < property.size) {
        val subEntities: Elements = property.get(i).parents.parents.get(0).getElementsByClass("wikibase-snakview-value-container")
        var links: String = ""
        var j: Int = 0
        while (j < subEntities.size) {
          val subEntityName: String = subEntities.get(j).child(1).text
          var subEntityLink: String = subEntities.get(j).child(1).childNode(0).attr("href")
          if (subEntityLink == null) {
            subEntityLink = ""
          }
          else if (subEntityLink.startsWith("/wiki/Q")) {
            temp = subEntityLink.split("/")
            subEntityLink = temp(temp.length - 1)
          }
          else if (subEntityLink.startsWith("/wiki/Property:")) {
            temp = subEntityLink.split("/")
            subEntityLink = temp(temp.length - 1)
          }
          else {
            subEntityLink = ""
          }
          links += subEntityName + Splitter.MICRO + subEntityLink
          links += Splitter.TINY
          weight.incrementAndGet

          j += 1
        }
        if (links.endsWith(Splitter.TINY)) {
          links = links.substring(0, links.length - Splitter.TINY.length)
        }
        content += property.get(i).child(0).text + Splitter.LIGHT + links
        content += Splitter.MEDIUM

        i += 1
      }
      if (content.endsWith(Splitter.MEDIUM)) {
        content = content.substring(0, content.length - Splitter.MEDIUM.length)
      }
      context.getDbOperator.addData(entity, content)
      context.getDbOperator.addEntityTo(context.CHECKEDENTITY, entity)
      context.getDbOperator.delEntityFrom(context.CHECKING, entity)
    }
    catch {
      case e: FileNotFoundException => {
        if (context.getDbOperator.searchEntityIn(context.CHECKEDENTITY, entity) == null) {
          context.getDbOperator.addEntityTo(context.CHECKEDENTITY, entity)
        }
        context.getDbOperator.delEntityFrom(context.CHECKING, entity)
      }
      case e: Exception => {
        e.printStackTrace
        targetUrl.decreaseRequestTime
        if (targetUrl.getRequestTime > 0) {
          context.getIndex.push(targetUrl)
          context.getNThread.incrementAndGet
          context.getExecutor.execute(this)
        }
        else {
          context.getDbOperator.addEntityTo(context.ENTITY, entity)
          context.getDbOperator.delEntityFrom(context.CHECKING, entity)
        }
      }
    } finally {
      context.getNThread.decrementAndGet
    }
  }
}
