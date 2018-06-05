package module.downloader

import java.io.FileNotFoundException

import module.Context
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.select.Elements
import util.{Requestor, Splitter, TargetUrl}

class SpreadDownloader(var context: Context) extends Downloader {

  // TODO: parse config
  override  def reset {
    context.getDbOperator.addEntityTo(context.ENTITY, "500021");
    context.getDbOperator.addEntityTo(context.ENTITY, "498722");
  }

  // TODO: parse config
  override def download {
    val entity : String = context.getDbOperator.getEntityFrom(context.ENTITY, 0)
    context.getDbOperator.delEntityFrom(context.ENTITY, entity)
    val targetUrl: TargetUrl = new TargetUrl(context.getWebEntityURLHead + entity)
    targetUrl.setRequestTime(context.getRequests)

    while(targetUrl.getRequestTime > 0) {
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
          {
            val subEntities: Elements = property.get(i).parents.parents.get(0).getElementsByClass("wikibase-snakview-value-container")
            var links: String = ""
            var j: Int = 0
            while (j < subEntities.size) {
              {
                val subEntityName: String = subEntities.get(j).child(1).text
                var subEntityLink: String = subEntities.get(j).child(1).childNode(0).attr("href")
                var temp : Array[String] = null
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
              }
              j += 1
            }
            if (links.endsWith(Splitter.TINY)) {
              links = links.substring(0, links.length - Splitter.TINY.length)
            }
            content += property.get(i).child(0).text + Splitter.LIGHT + links
            content += Splitter.MEDIUM
          }
          i += 1
        }
        if (content.endsWith(Splitter.MEDIUM)) {
          content = content.substring(0, content.length - Splitter.MEDIUM.length)
        }
        context.getDbOperator.addData(entity, content)
        context.getDbOperator.addEntityTo(context.CHECKEDENTITY, entity)
      }
      catch {
        case e: FileNotFoundException => {
          // page not exist
          context.getDbOperator.addEntityTo(context.CHECKEDENTITY, entity)
        }
        case e: Exception => {
          targetUrl.decreaseRequestTime
          if (targetUrl.getRequestTime <= 0) {
            context.getDbOperator.addEntityTo(context.ENTITY, entity)
          }
        }
      }
    }
  }
}
