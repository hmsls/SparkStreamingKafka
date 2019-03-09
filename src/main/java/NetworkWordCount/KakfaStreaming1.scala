package NetworkWordCount

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 官方不推荐直接用KafkaUtils.createStream，使用了receivers来接收数据，
  * 利用的是Kafka高层次的消费者api，对于所有的receivers接收到的数据将会保存在Spark executors中，
  * 然后通过Spark Streaming启动job来处理这些数据，默认会丢失，可启用WAL日志，它同步将接受到数据保存到分布式文件系统上比如HDFS。
  * 所以数据在出错的情况下可以恢复出来 。通过这种方式实现，刚开始的时候系统正常运行，没有发现问题，
  * 但是如果系统异常重新启动sparkstreaming程序后，发现程序会重复处理已经处理过的数据，这种基于receiver的方式，
  * 是使用Kafka的高阶API来在ZooKeeper中保存消费过的offset的。这是消费Kafka数据的传统方式。
  * 这种方式配合着WAL机制可以保证数据零丢失的高可靠性，但是却无法保证数据被处理一次且仅一次，可能会处理两次。
  * 因为Spark和ZooKeeper之间可能是不同步的。官方现在也已经不推荐这种整合方式
  */
object KakfaStreaming1 {
  def main(args: Array[String]): Unit = {
    //1、创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingKafka_Receiver").setMaster("local[2]")
    //2、创建sparkContext
//    val sc = new SparkContext(sparkConf)
//    sc.setLogLevel("INFO")
    //3、创建StreamingContext
    val ssc = new StreamingContext(sparkConf,Seconds(1))
    print("streaming context 已經創建好了。")
    //设置checkpoint
//    ssc.checkpoint("./Kafka_Receiver")
    //4、定义zk地址
    val zkQuorum="192.168.174.160:2181"
    //5、定义消费者组
    val groupId="spark_receiver"
    //6、定义topic相关信息 Map[String, Int]
    // 这里的value并不是topic分区数，它表示的topic中每一个分区被N个线程消费
    val topics=Map("kafka-stream2" -> 2)
    //7、通过KafkaUtils.createStream对接kafka
    val lines = KafkaUtils.createStream(ssc,zkQuorum,groupId,topics,StorageLevel.MEMORY_AND_DISK).map(_._2)
    print("讀取數據："+lines)
    val words = lines.flatMap(_.split(" "))
//    val wordCount = words.map(x=>(x,1)).reduceByKeyAndWindow(_+_,_-_,Seconds(5),Seconds(4),2)
    val wordCount = words.map(x=>(x,1)).reduceByKey(_+_)
    wordCount.print()
    //开启计算
    ssc.start()
    ssc.awaitTermination()
  }
}
