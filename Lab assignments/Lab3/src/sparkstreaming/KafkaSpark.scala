package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
    
  def main(args: Array[String]) {
            
    //Connect to Cassandra and make a keyspace and table as explained in assignment description.
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
  
    //Build a keyspace nad a Table in Cassandra.
    //KeySpace must be called: avg_space and Table must be called: avg      
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION =" +
                    "{'class': 'SimpleStrategy', 'replication_factor': 1};")
    //Table has two columns: text(being the key) and float(being the average value).
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);");

    //Kafka configuration:
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000"
    )
     
    //Spark configuration for streaming:
    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaAverageApplication") //Application name is mandatory in the configuration stage.
    
    //Streaming context from Spark.
    val ssc = new StreamingContext(conf, Seconds(1)) 
    ssc.checkpoint("file:///tmp/spark/checkpoint")
      
    //KafkaStream: keyclass, value class, key decoder class, value decoder class. Map.
    val topicsSet = "avg".split(",").toSet
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaConf,topicsSet)
    
    //Messages are using the format: key,value. Each value contains the word and the count
    //Example: <key,"z,15">. For this reason, we need to first ignore the key and just extract the
    //word, being the letter, and the number, from the value. 
    val pairs = messages.map(x => (x._2).split(",")).map(x =>(x(0),x(1).toDouble))
      
    //Calculate the average value per each key in a Statful manner:
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
        val oldState = state.getOption.getOrElse(0.0)
        //Way to calculate the average using just State[Double]
        val newState = (oldState + value.getOrElse(0.0))*0.5
        state.update(newState)
        return (key,newState)
    }
    
    //Save Cassandra:
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))
    //Save the result in Cassandra Table: keyspace, table, columns:
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))
    ssc.start()
    ssc.awaitTermination()
  }
}
