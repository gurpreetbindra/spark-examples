package com.spark.examples.scala.tut01.movieratings

import org.apache.spark.{SparkConf, SparkContext}

//Count number of movies for each rating


// Download data from https://grouplens.org/datasets/movielens/
//spark-submit --class com.spark.examples.scala.tut01.movieratings.MovieRatings ~/IdeaProjects/spark-examples/target/spark-examples-1.0-SNAPSHOT.jar

/* Expected results
34174 titles have rating of 4
21201 titles have rating of 5
6110 titles have rating of 1
11370 titles have rating of 2
27145 titles have rating of 3
 */

class MovieRatings {

  def run(sc: SparkContext): Unit = {
      val lines = sc.textFile("./data/spark/ml-100k/u.data")
      val ratings = lines.map(_.split("\\s+")(2))
      val count = ratings.countByValue()

      count.foreach(x => println(x._2 + " titles have rating of " + x._1))
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
