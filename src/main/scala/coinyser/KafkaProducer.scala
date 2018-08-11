package coinyser

import java.sql.Timestamp
import java.text.SimpleDateFormat

import cats.effect.IO
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.pusher.client.{Client, Pusher}
import com.pusher.client.channel.SubscriptionEventListener
import com.pusher.client.connection.{ConnectionEventListener, ConnectionState, ConnectionStateChange}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {
  def convertWsTransaction(wsTx: WebsocketTransaction): Transaction =
    Transaction(
      date = new Timestamp(wsTx.timestamp.toLong * 1000),
      tid = wsTx.id,
      price = wsTx.price,
      sell = wsTx.`type` == 1,
      amount = wsTx.amount)

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
  mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
  // can use sdf.setTimeZone(TimeZone.getTimeZone("UTC")), but not required: the time is in local time, but is also parsed in local time is spark

  def deserializeWebsocketTransaction(s: String): WebsocketTransaction = {
    mapper.readValue(s, classOf[WebsocketTransaction])
  }

  def serializeTransaction(tx: Transaction): String = {
    mapper.writeValueAsString(tx)
  }

  def subscribe(pusher: Client)(onTradeReceived: String => Unit): IO[Unit] =
    for {
      _ <- IO(pusher.connect())
      channel <- IO(pusher.subscribe("live_trades"))

      _ <- IO(channel.bind("trade", new SubscriptionEventListener() {
        override def onEvent(channel: String, event: String, data: String): Unit = {
          // TODO use logging
          println(s"Received event: $event with data: $data")
          onTradeReceived(data)
        }
      }))
    } yield ()


  def start(pusher: Client, kafkaProducer: KafkaProducer[String, String]): IO[Unit] =
    subscribe(pusher) { wsTx =>
      val tx = serializeTransaction(convertWsTransaction(deserializeWebsocketTransaction(wsTx)))
      // TODO pass topic in a context object
      kafkaProducer.send(new ProducerRecord[String, String]("transactions_draft1", tx))
    }


}
