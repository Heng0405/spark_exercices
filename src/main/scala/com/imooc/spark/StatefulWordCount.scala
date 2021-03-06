package main.scala.com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulWordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StatefulWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    ssc.checkpoint(".")
    val lines = ssc.socketTextStream("localhost",6789)
    val result = lines.flatMap(_.split("")).map((_,1))
    val state = result.updateStateByKey[Int](updateFunction _)

    //result.print()

    /*Insert into MySql*/
    ssc.start()
    ssc.awaitTermination()


    def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
      val newCount = newValues.sum
      val old = runningCount.getOrElse(0)
      Some(newCount+old)
    }
  }

}
