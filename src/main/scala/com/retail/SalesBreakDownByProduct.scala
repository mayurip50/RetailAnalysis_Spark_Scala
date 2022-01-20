package com.retail
import org.apache.spark.sql.SparkSession

object SalesBreakDownByProduct {

  def main(args:Array[String]): Unit ={


    val spark =SparkSession.builder().master("local").appName("wordcount").getOrCreate()
    val rdd=spark.read.textFile("D:\\course\\dataflair\\Retail_Sample_Data_Set.txt").rdd
val result=rdd.map(line=>{
 val l= line.split("\\t")
  (l(3),l(4).toFloat)
}).reduceByKey(_+_)

result.foreach(println)
  }
}
