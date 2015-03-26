import java.io.File

import scala.io.Source

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

object LinkourmetALS {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    if (args.length != 1) {
      println("Usage: YOUR_SPARK_HOME/bin/spark-submit --class LinkourmetALS target/scala-*/*.jar userID")
      sys.exit(1)
    }

    // set up environment

    val conf = new SparkConf().setAppName("LinkourmetALS")
    val sc = new SparkContext(conf)
    
    // load data

    val data = sc.textFile("ratings.dat").map(_.split("::") match { case Array(user, link, rate) =>
        Rating(user.toInt, link.toInt, rate.toDouble)
      })

    println("[XC] Data: ")
    data.collect().foreach(println)

    // Build the recommendation model using ALS

    val rank = 10
    val numIterations = 20
    val model = ALS.trainImplicit(data, 1, 5)

    // userData is RDD of (user, link) where 'link' is not shared by this user

    val userID = args(0).toInt

    val sharedLinks = data.filter(x => x.user == userID).map(_.product)
    val allLinks = data.map(_.product).distinct()
    val unsharedLinks = allLinks.subtract(sharedLinks)

    val userData = unsharedLinks.map((userID, _))

    println("[XC] User Data: ")
    userData.collect().foreach(println)

    // recommendation

    val predictions = model.predict(userData).collect().sortBy(-_.rating).take(50)
  
    println("[XC] Recommendation: ")
    predictions.foreach(println)

    // clean up
    sc.stop()
  }
} 
