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

import scala.math._

object LinkRec {

  final val DB_TABLE = "linkrec"
  final val TRAINING_RANK = 10
  final val TRAINING_NUM_ITERATIONS = 20
  final val RECOMMENDATION_NUMBER = 20

  val logger = Logger("LinkRec")

  def main(args: Array[String]) {

    if (args.length != 1) {
      println("Usage: YOUR_SPARK_HOME/bin/spark-submit --class LinkRec --master yarn-cluster target/scala-*/*.jar <userID>")
      sys.exit(1)
    }

    val sc = init()
    
    // load data, data in tuple (user: String, url: String, title: String, time: Long)
    val data = loadDataFromDB(sc)
    val targetUser = args(0)

    val ratingData = data.map(tuple => Rating(tuple._1, tuple._2, 1.0))
    val recommendation = predict(ratingData, targetUser)
    logger.debug(recommendation.mkString("\n"))

    val reclinks = rank(recommendation.map(_.product), data)

    var linkTitleMap = data.filter(tuple => reclinks.contains(tuple._2))
                           .map(tuple => (tuple._2, tuple._3))
                           .distinct()
                           .collectAsMap()

    var reclinksWithTitle = reclinks.map(url => 
                            "{\"url\":\"" + url + "\", \"title\":\"" + linkTitleMap.get(url).get + "\"}");

    print("{\"reclinks\": [" + reclinksWithTitle.mkString(", ") + "]}")

    sc.stop()
  }

  def init(): SparkContext = {
    logger.debug("start initializing spark")

    val conf = new SparkConf().setAppName("LinkRec")
    val sc = new SparkContext(conf)

    logger.debug("complete initializing spark")
    return sc
  }

  def loadDataFromDB(sc: SparkContext): RDD[(String, String, String, Long)] = {
    logger.debug("start loading data")

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, DB_TABLE)

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val ratings = hBaseRDD.map(_._2).map(_.raw())
                  .flatMap(_.map( cell => (
                          Bytes.toString(CellUtil.cloneRow(cell)), 
                          Bytes.toString(CellUtil.cloneQualifier(cell)),
                          Bytes.toString(CellUtil.cloneValue(cell)),
                          cell.getTimestamp()) ))
                  .cache()
    
    logger.debug("complete loading data")
    return ratings
  }

  def predict(ratings: RDD[Rating], user: String): Array[Rating] = {
    logger.debug("start predicting")

    var model = trainData(ratings)

    val sharedLinks = ratings.filter(_.user == user).map(_.product)
    val allLinks = ratings.map(_.product).distinct()
    val unsharedLinks = allLinks.subtract(sharedLinks)

    val userData = unsharedLinks.map((user, _))

    val prediction = model.predict(userData).collect().filter(_.rating >= 0).sortBy(-_.rating).take(RECOMMENDATION_NUMBER)

    logger.debug("complete predicting")
    return prediction
  }

  def trainData(data: RDD[Rating]): MatrixFactorizationModel = {
    logger.debug("start training data")

    val model = ALS.trainImplicit(data, TRAINING_RANK, TRAINING_NUM_ITERATIONS)

    logger.debug("complete training data")
    return model
  }

  def rank(urls: Array[String], data: RDD[(String, String, String, Long)]): Array[String] = {
    logger.debug("start ranking")

    val timeScore = data.filter(tuple => urls.contains(tuple._2))
         .map(tuple => (tuple._2, tuple._4))
         .reduceByKey( (a, b) => max(a, b) )
         .collect()
         .sortBy(-_._2)
         .map(_._1)
         .zipWithIndex
         .map(tuple => (tuple._1, tuple._2 * 0.25) )

    val ratingScore = urls.zipWithIndex.map(tuple => (tuple._1, tuple._2 * 0.75) )

    val rankedUrls = ( timeScore ++ ratingScore ).groupBy(_._1)
                                                    .mapValues(_.map(_._2).sum)
                                                    .toArray
                                                    .sortBy(_._2)
                                                    .map(_._1)

    logger.debug("complete ranking")
    return rankedUrls
  }

} 
