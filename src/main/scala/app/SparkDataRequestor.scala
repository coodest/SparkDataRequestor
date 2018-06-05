package app

import org.apache.spark._
import module.Context
import org.apache.spark.storage.StorageLevel

/**
  * Created by root on 6/27/16.
  */
object SparkDataRequestor{

  // TODO: parse config
  def main(args: Array[String]) {
    // 1. setup spark environment
    // NOTE: 1 Job[piece of program] -> n operation[like map, filter ...] ->
    // n RDD -> (nxm) task[m partition -> m DFS_Block]
    val conf = new SparkConf()
      .setAppName("DataRequestor")
//      .set("spark.executor.memory", "768m") // default 1g
//      .set("spark.executor.cores","2") // core per executor(must < phy cores), default 1 in YARN
//      .set("spark.task.cpus","1")  // core per task(partition), default 1
      //.set("spark.default.parallelism","1") // total task(partition), default Max(available cores in cluster, 2)
      //.set("spark.dynamicAllocation.enabled", "true") // major for YARN, auto executors
      //.set("spark.dynamicAllocation.minExecutors", "1") // major for YARN, default 0
      //.set("spark.dynamicAllocation.maxExecutors", "2") // major for YARN, default INF
      //.set("spark.shuffle.service.enabled", "true") // major for YARN, default false
      // A. YARN client (note: use only spark-submit for cluster mode)
      // NOTE: parallelism(partition) = total core = spark.executor.cores * spark.executor.instances
//      .setMaster("yarn-client")
//      //.set("spark.executor.instances","1") // manual executors, default 2
//      .set("spark.yarn.am.memoryOverhead","64") // default 384
//      .set("spark.yarn.am.memory", "256m") // default 512m
//      .set("spark.yarn.am.cores", "1") // cores for app master, default 1
      // B. Spark standalone
      // NOTE: parallelism(partition) != total core = worker * spark.executor.cores
//      .setMaster("spark://192.168.1.80:7077")
//      .set("spark.eventLog.enabled", "true") // default false
//      .set("spark.eventLog.dir", "file:///opt/spark-1.6.1-bin-hadoop2.4/logs")
      // C. Spark local
      .setMaster("local[2]")
    val sparkContext = new SparkContext(conf)

    // 2.init
//    val preHeatRDD = sparkContext.parallelize(1 to 1000) // preheat spark standalone allocate all cores
//    preHeatRDD.count() // preHeatRDD to make all resource allocated in time
    val probeRDD = sparkContext.parallelize(1 to 1000)
    val threads = probeRDD.partitions.length // MAY not be all cores granted course by allocation DELAY
    val driverContext: Context = new Context
    val refresh: Boolean = true
    val kind : Int = driverContext.QMCS
    val databaseAddress : String = "jdbc:mysql://192.168.48.128:3306/"
    val databaseName : String = "DataCollector"
    val tableName : String = "data"
    driverContext.setDatabaseAddress(databaseAddress)
    driverContext.setDatabaseName(databaseName)
    driverContext.setTableName(tableName)
    driverContext.driverBuild(refresh, kind)

    // 3. execute
    val pool = sparkContext.makeRDD(1 to threads, threads)
    val kindBC = sparkContext.broadcast(kind)
    val databaseAddressBC = sparkContext.broadcast(databaseAddress)
    val databaseNameBC = sparkContext.broadcast(databaseName)
    val tableNameBC = sparkContext.broadcast(tableName)
    val mappedPool = pool.map(num => {
      // A. YARN-Client: code execute in worker, so NO println, and could NOT call vars out of this field unless broadcast
      // B. Spark standalone: same as A.
      // C. Local: all code run at local with all feature defined including println
      val context: Context = new Context
      context.setDatabaseName(databaseNameBC.value)
      context.setTableName(tableNameBC.value)
      context.setDatabaseAddress(databaseAddressBC.value)
      //context.setMaxEntity(5000)
      context.setWebEntityURLHead("https://www.wikidata.org/wiki/")
      context.setEncoding("UTF-8")
      context.setMultiplier(1)
      context.setRequests(5)
      //context.setFromThat("2014-1-1")
      context.workerBuild(kindBC.value)

      while (context.getDbOperator.getTableSize(context.ENTITY) > 0) {
        context.getDownloader.download
      }
    })
    driverContext.release
    println("finish : " + mappedPool.count())
    
    // 4. cleanup spark environment
    sparkContext.stop()
  }
}
