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


import scala.collection.mutable.Queue

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
    val ssc = new StreamingContext(sparkContext, Seconds(1))
    ssc.checkpoint(".")


    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")


      case class Country_state(amountofdays: Int, population: Double) {
      private val datalist = new Queue[Int]()

      def addData(data: Int): Unit  = {

        val start = 5
        val end   = 15
        val rnd = new scala.util.Random
        val noise = start + rnd.nextInt( (end - start) + 1 )


        if (datalist.length < amountofdays){
          datalist.enqueue(data+noise)
        }
        else{
          datalist.dequeue
          datalist.enqueue(data+noise)
        }
      }

      def calculateAverage(): Double ={
        datalist.foldLeft(0)(_ + _)/datalist.length
      }

      def calculatecasedensity(): Double={
        datalist.foldLeft(0)(_ + _)/population*100000
      }

      def getsum(): Double={
        datalist.foldLeft(0)(_ + _)
      }

      def getpop(): Double={
        population
      }



    }

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
    val filter_new_cases = tojson.filter(x=> x._2.New_Cases_text != "")
    val filter_countries = filter_new_cases.filter(x => world_population.contains(x._2.Country_text.replace("_", " ")))


    def update_corona_cases(key: String, value: Option[CountryData], state: State[Country_state]): (String, Any) = {
      
      val data: CountryData = value.getOrElse(new CountryData("","","","","","","",""))
      val country = data.Country_text.replace("_", " ")
      val data_stored = 14
      


      //ROLLING AVERAGE LOGIC
      val country_state = state.getOption.getOrElse(new Country_state(data_stored, world_population(country).toDouble))
      val newCases = data.New_Cases_text.replace(",","").toInt
      country_state.addData(newCases)
      state.update(country_state)

      val current_average = country_state.calculateAverage()
      val casedensity = country_state.calculatecasedensity()
      //ROLLING AVERAGE LOGIC

      (country, (current_average, casedensity))

    }

    val pairs = filter_countries.mapWithState(StateSpec.function(update_corona_cases _))
    val order = pairs.transform(x => x.sortByKey())
    order.saveToCassandra("covid_space", "covid", SomeColumns("country", "corona_cases"))

    ssc.start()
    ssc.awaitTermination()

  }
}
