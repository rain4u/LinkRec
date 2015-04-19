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

case class Link(url: String, title: String, latestShared: Long, sharedTimes: Long)
case class Sharing(user: String, link: Link)
case class Recommendation(user: String, link: Link, rating: Double)

object LinkRec {

  final val DB_TABLE = "linkrec"
  final val COLUMN_FAMILY_LINK = "link"
  final val COLUMN_FAMILY_REC = "rec"
  final val COLUMN_RESULTS = "results"

  final val TRAINING_RANK = 10
  final val TRAINING_NUM_ITERATIONS = 20

  final val RECOMMENDATION_NUMBER = 20

  final val RANK_WEIGHT_RATING = 0.6
  final val RANK_WEIGHT_FRESHNESS = 0.25
  final val RANK_WEIGHT_POPULARITY = 0.15

  val logger = Logger("LinkRec")

  def main(args: Array[String]) {

    val sc = init()
    
    val data: RDD[Sharing] = loadDataFromDB(sc)

    val ratingData: RDD[Rating] = data.map(sharing => Rating(sharing.user, sharing.link.url, 1.0)).cache()
    val prediction: RDD[Rating] = predict(ratingData)

    val urlRatingPair = prediction.map(rating => ( rating.product, rating ) )
    val urlLinkPair = data.map(_.link).distinct().map(link => ( link.url, link ))
    val recommendation: RDD[Recommendation] = urlRatingPair.join(urlLinkPair)
                                                           .map(item => Recommendation( item._2._1.user, item._2._2, item._2._1.rating ))

    val usersRecommendation = recommendation.map(rec => (rec.user, rec))
                                            .combineByKey(rec => Array(rec),
                                                         (array: Array[Recommendation], rec) => array :+ rec,
                                                         (array1: Array[Recommendation], array2: Array[Recommendation]) => array1 ++ array2)
                                            .mapValues(rec => generateResult( rank( rec.sortBy(-_.rating).take(RECOMMENDATION_NUMBER) ) ))

    writeResultsToDB(usersRecommendation)

    sc.stop()
  }



  def init(): SparkContext = {
    logger.warn("start initializing spark")

    val conf = new SparkConf().setAppName("LinkRec")
    val sc = new SparkContext(conf)

    logger.warn("complete initializing spark")
    return sc
  }



  def loadDataFromDB(sc: SparkContext): RDD[Sharing] = {
    logger.warn("start loading data")

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, DB_TABLE)
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, COLUMN_FAMILY_LINK)

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    // (user: String, url: String, title: String, time: Long)
    val data: RDD[(String, String, String, Long)] = hBaseRDD.map(item => item._2.raw())
                                                               .flatMap(_.map( cell => (
                                                                       Bytes.toString(CellUtil.cloneRow(cell)), 
                                                                       Bytes.toString(CellUtil.cloneQualifier(cell)),
                                                                       Bytes.toString(CellUtil.cloneValue(cell)),
                                                                       cell.getTimestamp()) ))
                                                               .cache()

    val title = data.map(item => (item._2, item._3))
                    .distinct()
    val lastShared = data.map(item => (item._2, item._4))
                         .reduceByKey( (time1, time2) => max(time1, time2) )
    val sharedTimes = data.map(item => (item._2, 1))
                          .reduceByKey(_+_)

    val links = title.cogroup(lastShared, sharedTimes)
                     .map(item => ( item._1, Link( item._1, item._2._1.head, item._2._2.head, item._2._3.head ) ))
    val users = data.map(item => (item._2, item._1))

    val sharing = users.join(links)
                       .map(item => Sharing( item._2._1, item._2._2 ))
                       .cache()

    logger.warn("complete loading data")
    return sharing
  }



  def predict(ratings: RDD[Rating]): RDD[Rating] = {
    logger.warn("start predicting")

    val model = trainData(ratings)
    val usersNotSharedProducts = generateUsersNotSharedProducts(ratings)

    val prediction = model.predict(usersNotSharedProducts).filter(_.rating >= 0)

    logger.warn("complete predicting")
    return prediction
  }

  def trainData(ratings: RDD[Rating]): MatrixFactorizationModel = {
    logger.warn("start training data")

    val model = ALS.trainImplicit(ratings, TRAINING_RANK, TRAINING_NUM_ITERATIONS)

    logger.warn("complete training data")
    return model
  }

  def generateUsersNotSharedProducts(ratings: RDD[Rating]): RDD[(String, String)] = {
    val allProducts = ratings.map(_.product).distinct()
    val allUsers = ratings.map(_.user).distinct()
    val usersProducts = allUsers.cartesian(allProducts)

    val usersSharedProducts = ratings.map(rating => (rating.user, rating.product))
    val usersNotSharedProducts = usersProducts.subtract(usersSharedProducts)

    return usersNotSharedProducts
  }



  def rank(recommendation: Array[Recommendation]): Array[Recommendation] = {
    if (recommendation.size < 2) return recommendation

    val factorTuple = recommendation.map(rec => ( rec, rec.rating, rec.link.latestShared, rec.link.sharedTimes ))

    val ratingMin = factorTuple.minBy(_._2)._2
    val freshnessMin = factorTuple.minBy(_._3)._3
    val popularityMin = factorTuple.minBy(_._4)._4

    val tempScoreTuple = factorTuple.map(tuple => ( tuple._1, tuple._2 - ratingMin, tuple._3 - freshnessMin, tuple._4 - popularityMin ))

    val ratingMax = tempScoreTuple.maxBy(_._2)._2
    val freshnessMax = tempScoreTuple.maxBy(_._3)._3
    val popularityMax = tempScoreTuple.maxBy(_._4)._4

    val scaledScoreTuple = tempScoreTuple.map(tuple => ( tuple._1, tuple._2 / ratingMax, tuple._3 / freshnessMax, tuple._4 / popularityMax ))


    val overallScore = scaledScoreTuple.map(tuple => 
                            ( tuple._1, tuple._2 * RANK_WEIGHT_RATING + tuple._3 * RANK_WEIGHT_FRESHNESS + tuple._4 * RANK_WEIGHT_POPULARITY ))

    return overallScore.map(_._1)
  }



  def generateResult(recommendations: Array[Recommendation]): String = {
    var reclinksWithTitle = recommendations.map(rec =>
                              "{\"url\":\"" + rec.link.url + "\", \"title\":\"" + rec.link.title + "\"}");

    return "{\"reclinks\": [" + reclinksWithTitle.mkString(", ") + "]}"
  }



  def writeResultsToDB(results: RDD[(String, String)]) = {
    logger.warn("start writing results to DB")

    val conf = HBaseConfiguration.create()
    val table = new HTable(conf, DB_TABLE)

    results.coalesce(3)
    results.foreachPartition { partition =>
      val conf = HBaseConfiguration.create()
      val table = new HTable(conf, DB_TABLE)
      partition.foreach { rdd =>
        val put = new Put(Bytes.toBytes(rdd._1))
        put.add(Bytes.toBytes(COLUMN_FAMILY_REC), Bytes.toBytes(COLUMN_RESULTS), Bytes.toBytes(rdd._2))
        table.put(put)
      }
      table.flushCommits()
      table.close()
    }

    logger.warn("complete writing results")
  }
} 
