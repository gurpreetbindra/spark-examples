package com.spark.examples.scala.tut02.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

//spark-submit --class com.spark.examples.scala.tut02.keyvalue.AverageFriendsByAge ~/IdeaProjects/spark-examples/target/spark-examples-1.0-SNAPSHOT.jar

/* Expected results
Given the input:
0,Will,33,385
1,Jean,26,2
2,Hugh,55,221
3,Deanna,40,465
4,Quark,68,21
5,Gurps,33,2
6,Matt,40,5

The expected result is:
(55,221)
(33,193)
(40,235)
(26,2)
(68,21)
 */

class AverageFriendsByAge {

  def run(sc: SparkContext) {

    val lines = sc.textFile("./data/spark/fake/fakeFriends.csv")
    val ageFriends = lines.map(_.split(",")).map(x => (x(2).toInt, x(3).toInt)) //create tuples (age, no of friends)

    //create tuples (age, (no. of friends, count) and then reduce it by key (age)
    //mapValues is more efficient than using map when keys are not required to be processed.
    val totalsByAge = ageFriends.mapValues((_, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    val avgByAge = totalsByAge.mapValues(x => x._1/x._2)

    avgByAge.collect().foreach(println(_))

  }

}

object AverageFriendsByAge {
     def main(args: Array[String]) {
         val conf = new SparkConf().setAppName("AverageFriendsByAge").setMaster("local")
         val sc = new SparkContext(conf)

         val afba = new AverageFriendsByAge()
         afba.run(sc)

         sc.stop()

     }
}

