package com.retail

import org.apache.spark.sql.SparkSession

object TotalSalesOfAllStores {
  def main(args:Array[String]): Unit ={


    val spark =SparkSession.builder().master("local").appName("wordcount").getOrCreate()
    val rdd=spark.read.textFile("D:\\course\\dataflair\\Retail_Sample_Data_Set.txt").rdd
    val result=rdd.map(line=>{
      val l= line.split("\\t")
      ("total sales",l(4).toFloat)
    })

    val finalrdd=  result.reduceByKey(_+_)

    finalrdd.foreach(println)

    println("total no of sales : "+result.count())
  }
}
