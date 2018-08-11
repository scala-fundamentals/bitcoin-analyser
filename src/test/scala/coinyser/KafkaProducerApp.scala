package coinyser

import java.util.Properties

import coinyser.draft.BitstampReceiver
import com.pusher.client.Pusher
import com.pusher.client.connection.ConnectionEventListener
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import com.pusher.client.channel.SubscriptionEventListener
import com.pusher.client.connection.ConnectionState
import com.pusher.client.connection.ConnectionStateChange
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.duration._
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.JavaConversions._

// Use log compaction to ensure high availability ? (2 producers running concurrently)
// TODO use IOApp
object KafkaProducerApp extends App {


  val props = Map(
    ("bootstrap.servers", "localhost:9092"),
    ("acks", "all"),
    ("retries", 0),
    ("batch.size", 16384),
    ("linger.ms", 1),
    ("buffer.memory", 33554432),
    ("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
    ("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"))

  val producer = new KafkaProducer[String, String](props.mapValues(_.asInstanceOf[AnyRef]))

  val pusher = new Pusher("de504dc5763aeef9ff52")
  KafkaProducer.start(pusher, producer).unsafeRunSync()
  println("started")

  Thread.sleep(1000000)
}




