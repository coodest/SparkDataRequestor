package module

import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.AtomicInteger

import module.downloader._
import util._

class Context{
  val CTRL: Int = -1
  val SEQUENCE: Int = 0
  val SPREAD: Int = 1
  val MCS : Int = 2
  val QMCS : Int = 3
  val RANDOM : Int = 4
  val ENTITY: String = "Entity"
  val CHECKEDENTITY: String = "CheckedEntity"
  val CHECKING: String = "Checking"
  val BBQueue: String = "BranchAndBoundQueue"

  private var webEntityURLHead: String = null
  private var encoding: String = null
  private var multiplier: Int = 0
  private var processors: Int = 0
  private var executor: ForkJoinPool = null
  private var index: ConcurrentStack[TargetUrl] = null
  private var nThread: AtomicInteger = null
  private var downloader: Downloader = null
  private var dbOperator: DBOperator = null
  private var databaseAddress: String = null
  private var databaseName: String = null
  private var tableName: String = null
  private var fromThat: Date = null
  private var maxEntity: Int = 0
  private var requests: Int = 0
  private var requestor: Requestor = null

  def getRequestor: Requestor = {
    requestor
  }

  def setRequestor(requestor: Requestor) {
    this.requestor = requestor
  }

  def getMaxEntity: Int = {
     maxEntity
  }

  def setMaxEntity(maxEntity: Int) {
    this.maxEntity = maxEntity
  }

  def getRequests: Int = {
     requests
  }

  def setRequests(requests: Int) {
    this.requests = requests
  }

  def getFromThat: Date = {
     fromThat
  }

  def setFromThat(date: String) {
    try {
      this.fromThat = new SimpleDateFormat("yyyy-MM-dd").parse(date)
    }
    catch {
      case e: ParseException => {
        System.err.println(e)
      }
    }
  }

  def getTableName: String = {
     tableName
  }

  def setTableName(tableName: String) {
    this.tableName = tableName
  }

  def getDatabaseName: String = {
     databaseName
  }

  def setDatabaseName(databaseName: String) {
    this.databaseName = databaseName
  }

  def getDatabaseAddress: String = {
     databaseAddress
  }

  def setDatabaseAddress(databaseAddress: String) {
    this.databaseAddress = databaseAddress
  }

  def getDbOperator: DBOperator = {
     dbOperator
  }

  def setDbOperator(dbOperator: DBOperator) {
    this.dbOperator = dbOperator
  }

  def getWebEntityURLHead: String = {
     webEntityURLHead
  }

  def setWebEntityURLHead(webEntityURLHead: String) {
    this.webEntityURLHead = webEntityURLHead
  }

  def getEncoding: String = {
     encoding
  }

  def setEncoding(encoding: String) {
    this.encoding = encoding
  }

  def getMultiplier: Int = {
    multiplier
  }

  def setMultiplier(multiplier: Int) {
    this.multiplier = multiplier
  }

  def getProcessors: Int = {
    processors
  }

  def setProcessors(processors: Int) {
    this.processors = processors
  }

  def getExecutor: ForkJoinPool = {
    executor
  }

  def setExecutor(executor: ForkJoinPool) {
    this.executor = executor
  }

  def getIndex: ConcurrentStack[TargetUrl] = {
    index
  }

  def setIndex(index: ConcurrentStack[TargetUrl]) {
    this.index = index
  }

  def getNThread: AtomicInteger = {
    nThread
  }

  def setNThread(nThread: AtomicInteger) {
    this.nThread = nThread
  }

  def getDownloader: Downloader = {
     downloader
  }

  def setDownloader(downloader: Downloader) {
    this.downloader = downloader
  }

  def driverBuild(refresh : Boolean,kind: Int){
    build(refresh, kind)

    if(!refresh){
      while (dbOperator.getTableSize(CHECKING) > 0) {
        val checking: String = dbOperator.getEntityFrom(CHECKING, 0)
        dbOperator.addEntityTo(ENTITY, checking)
        dbOperator.delEntityFrom(CHECKING, checking)
      }
    }
  }

  def workerBuild(kind: Int){
    processors = Runtime.getRuntime.availableProcessors()
    executor = new ForkJoinPool(processors * multiplier)
    index = new ConcurrentStack[TargetUrl]
    nThread = new AtomicInteger
    requestor = new Requestor

    build(false, kind) // false because worker threads need NOT rebuild database
  }

  def build(refresh : Boolean,kind: Int) {
    kind match {
      case SEQUENCE =>
        downloader = new SequenceDownloader(this)
      case SPREAD =>
        downloader = new SpreadDownloader(this)
      case MCS =>
        downloader = new MCSDownloader(this)
      case QMCS =>
        downloader = new QMCSDownloader(this)
      case RANDOM =>
        downloader = new RandomDownloader(this)
      case _ =>
    }
    dbOperator = new DBOperator(databaseName, tableName, refresh, this)
    if(refresh){
      downloader.reset
    }
  }

  def release {
    dbOperator.close
  }
}
