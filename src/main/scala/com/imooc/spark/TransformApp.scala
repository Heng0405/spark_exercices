package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformApp {

  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("TransformApp")

    val ssc = new StreamingContext(sparkconf,Seconds(5))

    /**
      * construire black list
      * logs exemple Dstream : 20190503,david   20190504,valentin   20190606,marc
      * ==> (david : 20190503,david) (valentin : 20190504,valentin) (marc, 20190606,marc)
      * blacklist RDD : david, valentin    ==> (david, true)  (valentin: true)
      * log out : 20190606,marc
      * leftjoin : (david: [<20190503,david>,<true>]) (valentin: [<20190504,valentin>,<true>])
      *            (marc: [<marc, 20190606,marc>,<false>])  ==>tuple 1
      */
    val black = List("zs","ls")
    val blackrdd = ssc.sparkContext.parallelize(black).map(x=>(x,true))
    val lines = ssc.socketTextStream("localhost", 6789)
    val logs = lines.map(x=>(x.split(,)(1),x)).transform(rdd=>
      rdd.leftOuterJoin(blackrdd)
        .filter(x=>x._2._2.getOrElse(false)!=true))
        .map(x=>x._2._1)
    logs.print()



    ssc.start()
    ssc.awaitTermination()


  }
}
