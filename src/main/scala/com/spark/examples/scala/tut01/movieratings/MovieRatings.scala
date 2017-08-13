package com.spark.examples.scala.tut01.movieratings

import org.apache.spark.{SparkConf, SparkContext}

//Count number of movies for each rating


// Download data from https://grouplens.org/datasets/movielens/
//spark-submit --class com.spark.examples.scala.tut01.movieratings.MovieRatings ~/IdeaProjects/spark-examples/target/spark-examples-1.0-SNAPSHOT.jar

/* Expected results
4 titles have rating of 34174
5 titles have rating of 21201
1 titles have rating of 6110
2 titles have rating of 11370
3 titles have rating of 27145
 */

class MovieRatings {

  def run(sc: SparkContext): Unit = {
      val lines = sc.textFile("./data/spark/ml-100k/u.data")
      val ratings = lines.map(_.split("\\s+")(2))
      val count = ratings.countByValue()

      count.foreach(x => println(x._1 + " titles have rating of " + x._2))
  }
}


object MovieRatings {

  def main(args : Array[String]) {
    //Initialize spark context
    val conf = new SparkConf().setAppName("MovieRatings").setMaster("local")
    val sc = new SparkContext(conf)

    val movieRatings = new MovieRatings()
    movieRatings.run(sc)

    //stop the spark context
    sc.stop()
  }
}
