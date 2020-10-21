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
import com.fasterxml.jackson.databind._
import spray.json._
import scala.io._

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


    case class CountryData(Active_Cases_text: String, Country_text: String, Last_Update: String, New_Cases_text: String, New_Deaths_text: String, Total_Cases_text: String, Total_Deaths_text: String, Total_Recovered_text: String)
    case class World_Population(country: String, population: Int)

    object MyJsonProtocol extends DefaultJsonProtocol {
      implicit val CountryDataFormat = jsonFormat8(CountryData)
    }

    import MyJsonProtocol._

    val topics = Set("corona1")
    val mapper = new ObjectMapper();
    val factory = mapper.getJsonFactory()


    /*read world population*/
    
    var world_population = Map[String, String]()
    val data = Source.fromFile("world_population.csv")

    for (line <- data.getLines) {
        val cols = line.split(",")
        if(cols.size>1){
          val tmp : Map[String, String] = Map(cols(0) -> cols(1))
          world_population = world_population.++(tmp)
        }
    }
    data.close


    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)
    val tojson = messages.mapValues(x =>  ((x : String).parseJson).convertTo[CountryData])

    def update_corona_cases(key: String, value: Option[CountryData], state: State[(String, Double)]): (String, Double) = {
      val data: CountryData = value.getOrElse(new CountryData("","","","","","","",""))
      val country = data.Country_text.replace("_", " ")

      
      if(world_population.contains(country)){
        val total_population = world_population(country).toDouble
        val total_cases = data.Total_Cases_text.replace(",","").toDouble

        var total_recovered = 0.0
        if(data.Total_Recovered_text != "N/A"){
          total_recovered = data.Total_Recovered_text.replace(",","").toDouble
        }
        val active_cases = total_cases - total_recovered
        val cases_per_1000 = active_cases*100000/total_population
        state.update((key, cases_per_1000))
        (key,cases_per_1000)
      } else (

        (key, 0.0)
      )
      
    }

    val pairs = tojson.mapWithState(StateSpec.function(update_corona_cases _))
    pairs.print()
    pairs.saveToCassandra("covid_space", "covid", SomeColumns("country", "corona_cases"))

    ssc.start()
    ssc.awaitTermination()

  }
}
