import grizzled.slf4j.Logger

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes

object LinkRec {

  val logger = Logger("LinkRec")

  def main(args: Array[String]) {

    if (args.length != 1) {
      println("Usage: YOUR_SPARK_HOME/bin/spark-submit --class LinkRec --master yarn-cluster target/scala-*/*.jar <userID>")
      sys.exit(1)
    }

    // set up environment

    val conf = new SparkConf().setAppName("LinkRec")
    val sc = new SparkContext(conf)
    
    // load data, data in tuple (user: String, url: String, title: String, time: Long)
    logger.warn("start loading data")
    val data = loadDataFromDB(sc)
    logger.warn("complete loading data")
    // println("[XC] Data: ")
    // data.collect().foreach(println)

    // get training data

    val trainingData = data.map(tuple => Rating(tuple._1, tuple._2, 1.0))

    // println("[XC] trainingData: ")
    // trainingData.collect().foreach(println)

    // Build the recommendation model using ALS
    logger.warn("start training data")
    var model = getBestModel(trainingData)
    logger.warn("complete training data")
    // get target user id

    val userID = args(0) // String

    val sharedLinks = data.filter(_._1 == userID).map(_._2)
    val allLinks = data.map(_._2).distinct()
    val unsharedLinks = allLinks.subtract(sharedLinks)

    val userData = unsharedLinks.map((userID, _))

    // println("[XC] User Data: ")
    // userData.collect().foreach(println)

    // recommendation
    logger.warn("start prediction")
    val predictions = model.predict(userData).collect().filter(_.rating >= 0).sortBy(-_.rating).take(50)
    logger.warn(predictions.mkString("\n"))

    val reclinks = predictions.map(_.product)

    // ranking TODO
    logger.warn("start building map")
    var linkTitleMap = data.filter(tuple => reclinks.contains(tuple._2))
                           .map(tuple => (tuple._2, tuple._3))
                           .distinct()
                           .collectAsMap()
    
    logger.warn(linkTitleMap.mkString("\n"))

    var reclinksWithTitle = reclinks.map(url => 
                            "{\"url\":\"" + url + "\", \"title\":\"" + linkTitleMap.get(url).get + "\"}");

    print("{\"reclinks\": [" + reclinksWithTitle.mkString(", ") + "]}")

    // val reclinks = model.recommendProducts(userID, 50)
    // println(reclinks.mkString("\n"))

    // clean up
    sc.stop()
  }

  def loadDataFromDB(sc: SparkContext): RDD[(String, String, String, Long)] = {
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "linkrec")

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val ratings = hBaseRDD.map(_._2).map(_.raw())
                  .flatMap(_.map( cell => (
                          Bytes.toString(CellUtil.cloneRow(cell)), 
                          Bytes.toString(CellUtil.cloneQualifier(cell)),
                          Bytes.toString(CellUtil.cloneValue(cell)),
                          cell.getTimestamp()) ))
    
    return ratings.cache()
  }

  def getBestModel(data: RDD[Rating]): MatrixFactorizationModel = {
    val rank = 10
    val numIterations = 20
    val model = ALS.trainImplicit(data, rank, numIterations)
    return model;
  }
} 
