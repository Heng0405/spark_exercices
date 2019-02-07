package com.imooc.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ForeachRddApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StatefulWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint(".")
    val lines = ssc.socketTextStream("localhost", 6789)
    val result = lines.flatMap(_.split("")).map((_, 1)).reduceByKey(_ + _)


    result.print()

    /*Insert into MySql*/
    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
          val connection = createConnection()
          partitionOfRecords.foreach(record => {
            val sql = "insert into wordCount(word,wordcount) values ('"+record._1+"',"+record._2+")"
            connection.createStatement().execute(sql)
          })
          connection.close()

      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

  /* get mysql connection */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")

  }
}