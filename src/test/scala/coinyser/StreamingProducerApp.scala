package coinyser

import java.util.Properties

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

  lazy val pusher = new Pusher("de504dc5763aeef9ff52")

  pusher.connect(new ConnectionEventListener {
    def onConnectionStateChange(change: ConnectionStateChange): Unit = {
      println("State changed to " + change.getCurrentState + " from " + change.getPreviousState)
    }

    def onError(message: String, code: String, e: Exception): Unit = {
      println("There was a problem connecting!")
    }
  }, ConnectionState.ALL)

  val channel = pusher.subscribe("live_trades")
  channel.bind("trade", new SubscriptionEventListener() {
    override def onEvent(channel: String, event: String, data: String): Unit = {
      println(s"Received event: $event with data: $data")
      producer.send(new ProducerRecord[String, String]("transactions_draft0", data))
    }
  })

  Thread.sleep(100000)
}

object StreamingProducerApp extends App {

  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("StreamingProducer")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
  val stream = ssc.receiverStream(new BitstampReceiver())
  stream.print()
  ssc.start()
  ssc.awaitTermination()
}

class BitstampReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  @transient
  lazy val pusher = new Pusher("de504dc5763aeef9ff52")

  def onStart(): Unit = {
    pusher.connect(new ConnectionEventListener {
      def onConnectionStateChange(change: ConnectionStateChange): Unit = {
        println("State changed to " + change.getCurrentState + " from " + change.getPreviousState)
      }

      def onError(message: String, code: String, e: Exception): Unit = {
        println("There was a problem connecting!")
      }
    }, ConnectionState.ALL)

    val channel = pusher.subscribe("live_trades")
    channel.bind("trade", new SubscriptionEventListener() {
      override def onEvent(channel: String, event: String, data: String): Unit = {
        store(data)
      }
    })

  }

  def onStop(): Unit = {
    pusher.disconnect()
    pusher.unsubscribe("live_trades")
  }


}
