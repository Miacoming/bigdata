package com.atguigubigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}//spark的上下文环境

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {

    //TODO建立和spark框架的连接

    val saprkConf=new SparkConf().setMaster("local").setAppName("WordCount")//基础配置对象
    val sc=new SparkContext(saprkConf)//建立spark环境框架连接

    //TODO执行业务操作

    val lines = sc.textFile("datas")

    val words = lines.flatMap(_.split(" "))

    val wordToOne=words.map(
      word=>(word,1)
    )


    //spark框架提供了更多的功能，将分组和聚合用一个方法实现
    //reduceByKey相同的key的数据，可以对value进行reduce聚合
    //wordToOne.reduceByKey((x+y)=>x+y)
    val wordToCount=wordToOne.reduceByKey(_+_)

    //hello git4


    //5.将转换结果采集到控制台打印出来
    val array = wordToCount.collect()
    array.foreach(println)

    //TODO关闭连接
    sc.stop()
  }

}

//////////////////////////////////////////////////////