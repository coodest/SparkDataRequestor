package module.downloader

import module.Context
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.select.Elements
import util._
import java.io.FileNotFoundException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock

class RandomDownloader(var context: Context) extends Downloader with Runnable {
  private var gate: CountDownLatch = null
  private var rtLock: ReentrantLock = null

  override def reset {
    var i: Long = 1
    while (i <= 10) {
      context.getDbOperator.addEntityTo(context.ENTITY, "Q" + String.valueOf(i))

      i += 1
    }
  }

  override def download {
    gate = new CountDownLatch(1)
    rtLock = new ReentrantLock
    var i: Int = 0
    while (i < context.getProcessors) {
      makeJob

      i += 1
    }
    try {
      if (context.getExecutor.getPoolSize > 0) {
        gate.await
      }
    }
    catch {
      case e: InterruptedException => {
        e.printStackTrace
      }
    }
  }

  private def makeJob {
    val ranNum: Int = (Math.random * context.getDbOperator.getTableSize(context.ENTITY)).toInt
    val entity: String = context.getDbOperator.getEntityFrom(context.ENTITY, ranNum)
    if (entity != null) {
      context.getDbOperator.delEntityFrom(context.ENTITY, entity)
      context.getDbOperator.addEntityTo(context.CHECKING, entity)
      val targetUrl: TargetUrl = new TargetUrl(context.getWebEntityURLHead + entity)
      targetUrl.setRequestTime(context.getRequests)
      context.getIndex.push(targetUrl)
      context.getNThread.incrementAndGet
      context.getExecutor.execute(this)
    }
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
      rtLock.lock
      makeJob
      rtLock.unlock
    }
    catch {
      case e: FileNotFoundException => {
        context.getDbOperator.addEntityTo(context.CHECKEDENTITY, entity)
        context.getDbOperator.delEntityFrom(context.CHECKING, entity)
        rtLock.lock
        makeJob
        rtLock.unlock
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
          try {
            context.getDbOperator.addEntityTo(context.ENTITY, entity)
            context.getDbOperator.delEntityFrom(context.CHECKING, entity)
          }
          catch {
            case ex: Exception => {
              ex.printStackTrace()
            }
          }
        }
      }
    } finally {
      if (context.getNThread.decrementAndGet == 0) {
        gate.countDown()
      }
    }
  }
}
