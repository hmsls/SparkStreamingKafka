package NetworkWordCount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by LISHUAI on 2018/12/15.
  */
object NetworkWordCount {
  def main(args: Array[String]):Unit = {
//    if(args.length < 2){
//      System.err.println("Useage : NetworkWordCount <hostname> <port>")
//      System.exit(1)
//    }
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.apachespark.streaming").setLevel(Level.WARN)
    //设置批数据的时间片大小为1秒
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    //使用输入的host和port构建Socket流，并设置存储级别
    //构建后得到的DStream实例
    val v1 = "192.168.174.160"
    val v2 = "9999"
    val lines = ssc.socketTextStream(v1,v2.toInt,StorageLevel.MEMORY_AND_DISK)
    //DStream提供了和RDD类似的high-level的API对内部的RDD序列进行处理
    //下面的处理方式和RDD的单词统计是一样的
    val words = lines.flatMap(line=>line.split(" "))
    val wordCount = words.map(word=>(word,1)).reduceByKey(_+_)
    wordCount.print()
    //最后调用start来正式启动流处理
    ssc.start()
    ssc.awaitTermination()
  }

}
