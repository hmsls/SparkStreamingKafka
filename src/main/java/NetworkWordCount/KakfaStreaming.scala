package NetworkWordCount

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 不同于Receiver接收数据，这种方式定期地从kafka的topic下对应的partition中查询最新的偏移量，
  * 再根据偏移量范围在每个batch里面处理数据，Spark通过调用kafka简单的消费者Api读取一定范围的数据。
  * 不需要创建多个kafka输入流，然后union它们，sparkStreaming将会创建和kafka分区一种的rdd的分区数，
  * 而且会从kafka中并行读取数据，spark中RDD的分区数和kafka中的分区数据是一一对应的关系。
  * 第一种实现数据的零丢失是将数据预先保存在WAL中，会复制一遍数据，会导致数据被拷贝两次，
  * 第一次是被kafka复制，另一次是写到WAL中。而没有receiver的这种方式消除了这个问题。
  * Receiver读取kafka数据是通过kafka高层次api把偏移量写入zookeeper中，虽然这种方法可以通过数据保存在WAL中保证数据不丢失，
  * 但是可能会因为sparkStreaming和ZK中保存的偏移量不一致而导致数据被消费了多次。EOS通过实现kafka低层次api，
  * 偏移量仅仅被ssc保存在checkpoint中，消除了zk和ssc偏移量不一致的问题。缺点是无法使用基于zookeeper的kafka监控工具.
  */
object KakfaStreaming {
  //配置kafka相关参数
  val kafkaParams=Map("metadata.broker.list"->"192.168.174.160:9092","group.id"->"Kafka_Direct")
  //定义topic
  val topics=Set("kafka-stream2")
  //检查点目录
  val checkpointDirectory="kafkaStream"

  //如果没从checkpoint中创建ssc，那就从开始创建ssc
  def functionToCreateContext():StreamingContext={
    //创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingKafka_Direct").setMaster("local[2]")
    //创建sparkContext
    val sc = new SparkContext(sparkConf)
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.stream").setLevel(Level.WARN)
    sc.setLogLevel("WARN")
    //创建StreamingContext
    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint(checkpointDirectory)
    ssc
  }

  def main(args: Array[String]): Unit = {
    val context = StreamingContext.getOrCreate(checkpointDirectory,functionToCreateContext _)
    //通过 KafkaUtils.createDirectStream接受kafka数据，这里采用是kafka低级api偏移量不受zk管理
    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](context,kafkaParams,topics)
    //获取kafka中topic中的数据
    val topicData: DStream[String] = dstream.map(_._2)
    //切分每一行,每个单词计为1
    val wordAndOne: DStream[(String, Int)] = topicData.flatMap(_.split(" ")).map((_,1))
    //相同单词出现的次数累加
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //用当前batch的数据去更新已有的数据
    wordAndOne.updateStateByKey[Int](updateFunc)
    //打印输出
    result.print()
    //开启计算
    context.start()
    context.awaitTermination()
  }
  //更新状态方法
  val updateFunc = (currentValue:Seq[Int],preValue:Option[Int])=>{
    val curr = currentValue.sum
    val prev = preValue.getOrElse(0)
    Some(curr+prev)
  }

}
