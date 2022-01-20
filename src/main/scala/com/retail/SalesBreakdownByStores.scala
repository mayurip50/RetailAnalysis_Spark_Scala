package com.retail

import org.apache.spark.sql.SparkSession

object SalesBreakdownByStores {
  def main(args:Array[String]): Unit ={


    val spark =SparkSession.builder().master("local").appName("wordcount").getOrCreate()
    val rdd=spark.read.textFile("D:\\course\\dataflair\\Retail_Sample_Data_Set.txt").rdd
    val result=rdd.map(line=>{
      val l= line.split("\\t")
      (l(2),l(4).toFloat)
    }).reduceByKey(_+_)

    result.foreach(println)
  }
}
