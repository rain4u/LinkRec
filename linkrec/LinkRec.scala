import grizzled.slf4j.Logger

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes

import scala.math._

object LinkRec {

  final val DB_TABLE = "linkrec"
  final val TRAINING_RANK = 10
  final val TRAINING_NUM_ITERATIONS = 20
  final val RECOMMENDATION_NUMBER = 20
  final val RANK_WEIGHT_SHARE_TIME = 0.25
  final val RANK_WEIGHT_PREDICT_RATING = 0.75

  val logger = Logger("LinkRec")

  def main(args: Array[String]) {

    val sc = init()
    
    // load data, data in tuple (user: String, url: String, title: String, time: Long)
    val data = loadDataFromDB(sc)
    val ratingData = data.map(tuple => Rating(tuple._1, tuple._2, 1.0)).cache()
    val linkToTitle = data.map(tuple => (tuple._2, tuple._3)).distinct().collect().toMap

    val allUsers = data.map(_._1).distinct()
    val allRecLinks = allUsers.map(user => (user, predict(ratingData, user)))
                              .map(tuple => (tuple._1, rank(tuple._2, data)))

    val allResults = allRecLinks.map(tuple => (tuple._1, generateResult(tuple._2, linkToTitle)))

    writeResultsToDB(sc, allResults)

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

    val prediction = model.predict(userData).filter(_.rating >= 0).sortBy(_.rating, false).take(RECOMMENDATION_NUMBER)

    logger.debug("complete predicting; prediction result\n" + prediction.mkString("\n"))
    return prediction
  }

  def trainData(data: RDD[Rating]): MatrixFactorizationModel = {
    logger.debug("start training data")

    val model = ALS.trainImplicit(data, TRAINING_RANK, TRAINING_NUM_ITERATIONS)

    logger.debug("complete training data")
    return model
  }



  def rank(recommendation: Array[Rating], metadata: RDD[(String, String, String, Long)]): Array[String] = {
    logger.debug("start ranking")

    val reclinks = rankByScaledValue(recommendation, metadata)

    logger.debug("complete ranking")
    return reclinks
  }

  def rankByIndex(recommendation: Array[Rating], metadata: RDD[(String, String, String, Long)]): Array[String] = {
    val urls = recommendation.map(_.product)
    val timeScore = metadata.filter(tuple => urls.contains(tuple._2))
         .map(tuple => (tuple._2, tuple._4))
         .reduceByKey( (a, b) => max(a, b) )
         .collect()
         .sortBy(-_._2)
         .map(_._1)
         .zipWithIndex
         .map(tuple => (tuple._1, tuple._2 * RANK_WEIGHT_SHARE_TIME) )

    val ratingScore = urls.zipWithIndex.map(tuple => (tuple._1, tuple._2 * RANK_WEIGHT_PREDICT_RATING) )

    val overallScore = ( timeScore ++ ratingScore ).groupBy(_._1)
                                                    .mapValues(_.map(_._2).sum)
                                                    .toArray
                                                    .sortBy(_._2)
    
    logger.debug("overallScore\n" + overallScore.mkString("\n"))
    return overallScore.map(_._1)
  }

  def rankByScaledValue(recommendation: Array[Rating], metadata: RDD[(String, String, String, Long)]): Array[String] = {
    val predictRatings = recommendation.map(item => (item.product, item.rating) )
    val scaledPredictRatings = scale(predictRatings)
    val ratingScore = scaledPredictRatings.map(tuple => (tuple._1, tuple._2 * RANK_WEIGHT_PREDICT_RATING))

    val urls = recommendation.map(_.product)
    val lastSharedTimes = metadata.filter(tuple => urls.contains(tuple._2))
                                 .map(tuple => (tuple._2, tuple._4.toDouble))
                                 .reduceByKey( (a, b) => max(a, b) )
                                 .sortBy(_._2, false)
                                 .collect()
    val scaledLastSharedTimes = scale(lastSharedTimes)
    val shareTimeScore = scaledLastSharedTimes.map(tuple => (tuple._1, tuple._2 * RANK_WEIGHT_SHARE_TIME))

    val overallScore = ( ratingScore ++ shareTimeScore ).groupBy(_._1)
                                                      .mapValues(_.map(_._2).sum)
                                                      .toArray
                                                      .sortBy(-_._2)

    logger.debug("overallScore\n" + overallScore.mkString("\n"))
    return overallScore.map(_._1)
  }

  def scale(input: Array[(String, Double)]): Array[(String, Double)] = {
    val min = input.minBy(_._2)._2
    val temp = input.map(tuple => (tuple._1, tuple._2 - min))
    val max = temp.maxBy(_._2)._2
    val ret = temp.map(tuple => (tuple._1, tuple._2 / max))
    return ret
  }

  def generateResult(reclinks: Array[String], linkToTitle: Map[String, String]): String = {
    var reclinksWithTitle = reclinks.map(url =>
                            "{\"url\":\"" + url + "\", \"title\":\"" + linkToTitle.get(url).get + "\"}");

    return "{\"reclinks\": [" + reclinksWithTitle.mkString(", ") + "]}"
  }

  def writeResultsToDB(sc: SparkContext, results: RDD[(String, String)]) = {
    logger.warn("start writing results to DB")

    val conf = HBaseConfiguration.create()
    val table = new HTable(conf, DB_TABLE)

    logger.warn(results.partitioner.size)
    results.coalesce(3)
    results.foreachPartition { partition =>
      val conf = HBaseConfiguration.create()
      val table = new HTable(conf, DB_TABLE)
      partition.foreach { rdd =>
        val put = new Put(Bytes.toBytes(rdd._1))
        put.add(Bytes.toBytes("rec"), Bytes.toBytes("results"), Bytes.toBytes(rdd._2))
        table.put(put)
      }
      table.flushCommits()
      table.close()
    }

    logger.warn("complete writing results")
  }
} 
