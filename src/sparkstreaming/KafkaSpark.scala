package sparkstreaming

import java.util.HashMap
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

import java.nio.ByteBuffer
import scala.math.BigInt

//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float)")

    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = Map[String, String](
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeper.connection.timeout.ms" -> "1000"
    )
      
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaTest") //local[2] means two threads
    val ssc = new StreamingContext(sparkConf, Seconds(2)) //
    ssc.checkpoint("./checkpoint")

    val topicsSet = "avg".split(",").toSet
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaConf, topicsSet
    )

    val inputValues = messages.map( x => x._2.split(","))
    val pairs = inputValues.map(x => (x(0), x(1).toDouble))
    //val pairs = words.map(x=>(x,1))


    //val charCountMap = scala.collection.mutable.Map[String,(Double, Int)]() //value,count
    val charCountMap = scala.collection.mutable.HashMap[String, (Double, Int)]()
    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
      val toDoubleVal: Double = value.getOrElse(0);
      if(charCountMap.contains(key)){
	      charCountMap(key) = (charCountMap(key)._1+toDoubleVal, charCountMap(key)._2+1);
      }else{
	      charCountMap(key) = (toDoubleVal, 1);
      }

      return (key, charCountMap(key)._1/charCountMap(key)._2)
    }

    // Initial state RDD for current models
    //val modelsRDD = ssc.sparkContext.emptyRDD[(String, Double)]

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _ ))//.initialState(modelsRDD))

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    ssc.start()
    ssc.awaitTermination()
  }
}
