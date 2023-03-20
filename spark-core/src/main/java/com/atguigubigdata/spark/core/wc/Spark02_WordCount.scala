package com.atguigubigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}//spark的上下文环境

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    //Application
    //spark框架、应用程序，连接spark框架，功能传递使其运行
    //TODO建立和spark框架的连接
    //JDBC:connection
    val saprkConf=new SparkConf().setMaster("local").setAppName("WordCount")//基础配置对象
    val sc=new SparkContext(saprkConf)//建立spark环境框架连接

    //TODO执行业务操作


    //1.读取文件，获取一行一行的数据
    // hello world
    val lines = sc.textFile("datas")




    //2.将一行数据拆分，分词效果
    //扁平化，将整体拆分成个体的操作
    //“hello world"=>hello,world,hello.world
    val words = lines.flatMap(_.split(" "))

    val wordToOne=words.map(
      word=>(word,1)
    )


    //3.将数据单词分组，便于统计
    //(hello,hello,hello),(world,world)
    val wordGroup=wordToOne.groupBy(
      t=>t._1
    )


    //4.对分组的数据进行转化
    //（hello,3),(world,2)
    val wordToCount=wordGroup.map {
      case (word,list)=>{

         list.reduce(
           (t1,t2)=>{
             (t1._1,t1._2+t2._2)
          }
        )
      }
    }

    //5.将转换结果采集到控制台打印出来
    val array = wordToCount.collect()
    array.foreach(println)

    //TODO关闭连接
    sc.stop()
  }

}
