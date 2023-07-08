package main.scala
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConverters._
import play.api.libs.json._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration

import java.time.{Instant, LocalDate, LocalTime}
import java.sql.Timestamp

object FirstSparkApp {
  def main(args: Array[String]): Unit = {

    System.setProperty("SPARK_LOCAL_IP", "192.168.56.106")
    System.setProperty("HADOOP_CONF_DIR", "/home/hdoop/hadoop-3.3.4/etc/hadoop")
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://192.168.56.106:9000")

    val spark = SparkSession.builder()
      .config("spark.hadoop.fs.defaultFS", hadoopConf.get("fs.defaultFS"))
      .config("spark.master", "local")
      .appName("HiveTableLoadJob")
      .config("hive.metastore.uris", "thrift://sagar-data:9083")
      .enableHiveSupport()
      .getOrCreate()

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sagar-data:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(List("data-stream").asJava)

    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) {
       // println(s"Received message: key = ${record.key()}, value = ${record.value()}, partition = ${record.partition()}, offset = ${record.offset()}")
      val data=record.value()

        val json: JsValue = Json.parse(data)
        val currentDate = LocalDate.now()
        val currentTime = Timestamp.from(Instant.now())
        val base = (json \ "data" \ "base").as[String]
        val currency = (json \ "data" \ "currency").as[String]
        val amount = (json \ "data" \ "amount").as[String]

        println(s"Base: $base")
        println(s"Currency: $currency")
        println(s"Amount: $amount")
        println(s"Current Date: $currentDate")
        println(s"Current Time: $currentTime")

        spark.sql("USE mydefault")
        spark.sql(
          """
          CREATE TABLE IF NOT EXISTS bitcoinprice (
            base STRING,
            currency STRING,
            amount STRING,
            currentDate DATE,
            currentTime TIMESTAMP
          )
          STORED AS PARQUET
        """)

        spark.sql(
          s"""
        INSERT INTO bitcoinprice
        VALUES ('$base', '$currency', '$amount', CAST('$currentDate' AS DATE), CAST('$currentTime' AS TIMESTAMP))
        """
        )

      }
    }
  }
}



// ======================================================================================
//package main.scala
//import java.util.Properties
//import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
//import scala.collection.JavaConverters._
//import play.api.libs.json._
//import org.apache.spark.sql.SparkSession
//import org.apache.hadoop.conf.Configuration
//import java.time.{LocalDate, LocalTime}
// import java.sql.Timestamp
//
//object FirstSparkApp {
//  def main(args: Array[String]): Unit = {
//
//    System.setProperty("SPARK_LOCAL_IP", "192.168.56.106")
//    System.setProperty("HADOOP_CONF_DIR", "/home/hdoop/hadoop-3.3.4/etc/hadoop")
//    val hadoopConf = new Configuration()
//    hadoopConf.set("fs.defaultFS", "hdfs://192.168.56.106:9000")
//
//    val spark = SparkSession.builder()
//      .config("spark.hadoop.fs.defaultFS", hadoopConf.get("fs.defaultFS"))
//      .config("spark.master", "local")
//      .appName("HiveTableLoadJob")
//      .config("hive.metastore.uris", "thrift://sagar-data:9083")
//      .enableHiveSupport()
//      .getOrCreate()
//
//    val props = new Properties()
//    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sagar-data:9092")
//    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
//    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
//    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group")
//    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//
//    val consumer = new KafkaConsumer[String, String](props)
//    // consumer.
//    consumer.subscribe(List("data-stream").asJava)
//
//    while (true) {
//      val records = consumer.poll(100)
//      for (record <- records.asScala) {
//        // println(s"Received message: key = ${record.key()}, value = ${record.value()}, partition = ${record.partition()}, offset = ${record.offset()}")
//        val data=record.value()
//
//        val json: JsValue = Json.parse(data)
//        val currentDate = LocalDate.now()
//        val currentTime = Timestamp.from(Instant.now())
//
//        val base = (json \ "data" \ "base").as[String]
//        val currency = (json \ "data" \ "currency").as[String]
//        val amount = (json \ "data" \ "amount").as[String]
//
//        println(s"Base: $base")
//        println(s"Currency: $currency")
//        println(s"Amount: $amount")
//        println(s"Current Date: $currentDate")
//        println(s"Current Time: $currentTime")
//
//      }
//    }
//  }
//}


// ====================================================================================
// remote Working code

//
//package main.scala
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
//
//object FirstSparkApp {
//  def main(args: Array[String]): Unit = {
//    val x = "hdfs://192.168.56.106:9000/tmp/abc1.txt"
//    val conf = new SparkConf().setAppName("HelloSpark").setMaster("local[*]") // Modified setMaster value
//    val sc = new SparkContext(conf)
//    val y = sc.textFile(x).cache()
//    val counts = y.flatMap(line => line.split(" "))
//      .map(word => (word, 1))
//      .reduceByKey(_ + _)
//    counts.saveAsTextFile("hdfs://192.168.56.106:9000/tmp/spark/sparkoutput" + java.util.UUID.randomUUID.toString) // Corrected output path
//    println("Code Executed")
//  }
//}

// =======================================================================
//  Local code to run
//package main.scala
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
//
//object FirstSparkApp {
//  def main(args: Array[String]): Unit = {
//    val x = "abc1.txt"
//    val conf = new SparkConf().setAppName("HelloSpark").setMaster("local[*]") // Modified setMaster value
//    val sc = new SparkContext(conf)
//    val y = sc.textFile(x).cache()
//    val counts = y.flatMap(line => line.split(" "))
//      .map(word => (word, 1))
//      .reduceByKey(_ + _)
//    counts.saveAsTextFile("sparkoutput" + java.util.UUID.randomUUID.toString) // Corrected output path
//    println("Code Executed")
//  }
//}




// ===================================================================================================

// Local Runnable code wordcount
//package main.scala
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
//
//object FirstSparkApp  {
//  def main(args: Array[String]): Unit = {
//    val x="abc1.txt"
//    val conf = new SparkConf().setAppName("HelloSpark").setMaster("local")
//    val sc=new SparkContext(conf)
//    val y=sc.textFile(x).cache()
//    val counts=y.flatMap(line=>line.split(" "))
//      .map(word=>(word,1))
//      .reduceByKey(_+_)
//    counts.saveAsTextFile("sparkoutpt"+java.util.UUID.randomUUID.toString)
//    println("Code Executed")
//  }
//
//}


// ===================================================================================================================
// Streaming Job
// sagar@sagar-Lenovo-ideapad-330-15IKB:~$ nc -lvp 2222
//
//package main.scala
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming._
//object FirstSparkApp {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("HeySparkStreaming")
//    val sc=new SparkContext(conf)
//    val scc = new StreamingContext(sc,Seconds(10))
//    val streamRDD = scc.socketTextStream("127.0.0.1",2222)
//    val wordcounts = streamRDD.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
//    wordcounts.print()
//    println(wordcounts.print())
//    scc.start()
//    scc.awaitTermination()
//  }
//}

// ========================================================

// Window Bases Streaming Job
// sagar@sagar-Lenovo-ideapad-330-15IKB:~$ nc -lvp 2222
//

// Window Based Streaming
//package main.scala
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming._
//import org.apache.spark.api.java.function._
//import org.apache.spark.storage.StorageLevel
//object FirstSparkApp {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("HeySparkStreaming").setMaster("local[2]")
//    val sc=new SparkContext(conf)
//    val scc = new StreamingContext(sc,Seconds(10))
//
//    val streamRDD = scc.socketTextStream("127.0.0.1",2222,StorageLevel.MEMORY_ONLY)
//
//    val wordcounts = streamRDD.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(30),Seconds(10))
//    wordcounts.print()
//    scc.start()
//    scc.checkpoint(".")
//    scc.awaitTermination()
//  }
//}



// =============================================================================================================================

// Including yarn with hdfs wordcount
//
//package main.scala
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
//
//object FirstSparkApp  {
//  def main(args: Array[String]): Unit = {
//    val x="hdfs://localhost:9000/tmp/abc1.txt"
//    System.setProperty("HADOOP_CONF_DIR", "/home/hdoop/hadoop-3.3.4/etc/hadoop")
//    val conf = new SparkConf()
//      .setAppName("HelloSpark")
//      .setMaster("yarn")
//      .set("spark.executor.memory", "2g")
//      .set("spark.executor.cores", "2")
//      .set("spark.driver.memory", "1g")
//      .set("spark.driver.cores", "1")
//      .set("spark.hadoop.yarn.resourcemanager.hostname", "localhost")
//    .setMaster("spark://sagar-data:7077")
//
//    val sc=new SparkContext(conf)
//    val y=sc.textFile(x).cache()
//    val counts=y.flatMap(line=>line.split(" "))
//      .map(word=>(word,1))
//      .reduceByKey(_+_)
//    counts.saveAsTextFile("hdfs://localhost:9000/tmp/spark/sparkoutpt"+java.util.UUID.randomUUID.toString)
//    println("Code Executed")
//  }
//}