package finalproject

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import com.fasterxml.jackson.databind.JsonNode
import spray.json._


import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

import org.apache.spark.SparkContext

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS covid_space WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
    session.execute("CREATE TABLE IF NOT EXISTS covid_space.covid (country text PRIMARY KEY, corona_cases text);")


    // make a connection to Kafka and read (key, value) pairs from it
    val conf = new SparkConf().setAppName("finalproject").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)
    val ssc = new StreamingContext(sparkContext, Seconds(5))
    ssc.checkpoint(".")


    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")


    val topics = Set("corona")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)

    val tojson = messages.mapValues(x => (x : String).parseJson)


    def update_corona_cases(key: String, value: Option[JsValue], state: State[(String, JsValue)]): (String, JsValue) = {
        val json : JsValue = value.getOrElse("""{"error":"error"}""".parseJson)
        state.update((key,json))
        (key,json)
    }

    val pairs = tojson.mapWithState(StateSpec.function(update_corona_cases _))
    pairs.print()
  
    pairs.saveToCassandra("covid_space", "covid", SomeColumns("country", "corona_cases"))

    ssc.start()
    ssc.awaitTermination()

  }
}
