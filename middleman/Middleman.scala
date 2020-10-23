

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import kafka.producer.KeyedMessage
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import java.util.{Properties}
object Middleman extends App {


    @throws(classOf[java.io.IOException])
    @throws(classOf[java.net.SocketTimeoutException])
    def get(url: String,
            connectTimeout: Int = 5000,
            readTimeout: Int = 5000,
            requestMethod: String = "GET") =
    {
        import java.net.{URL, HttpURLConnection}
        val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
        connection.setConnectTimeout(connectTimeout)
        connection.setReadTimeout(readTimeout)
        connection.setRequestMethod(requestMethod)
        val inputStream = connection.getInputStream
        val content = io.Source.fromInputStream(inputStream).mkString
        if (inputStream != null) inputStream.close
        content
    }

    val topic = "corona1"
    val brokers = "localhost:9092"

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "Scala coronaAPI middleman")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

        try {
            val slug = "covid-update"
            val url = "https://covid-19.dataflowkit.com/v1"
            val content = get(url).replace(" ","_")
            val mapper = new ObjectMapper()
            val node = mapper.readTree(content);
            while(true){
            for (x <- List.range(0, node.size-1)){

                val countryjson = node.get(x)
                val country = countryjson.get("Country_text")
                val last_update = countryjson.get("Last_Update")
                if (last_update != null){
                val data = new ProducerRecord[String, String](topic, country.toString, countryjson.toString)
                producer.send(data)
                println(data)
                }
            }

            Thread.sleep(30000)
          }
        } catch {
            case ioe: java.io.IOException =>  // handle this
            case ste: java.net.SocketTimeoutException => // handle this
        }

    producer.close()
}
