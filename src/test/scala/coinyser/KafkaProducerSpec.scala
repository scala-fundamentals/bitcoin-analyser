package coinyser

import java.sql.Timestamp

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.{BeforeAndAfterAll, EitherValues, Matchers, WordSpec}
import org.scalatest.concurrent.Eventually
import KafkaProducerSpec._

class KafkaProducerSpec extends WordSpec with Matchers with BeforeAndAfterAll with TypeCheckedTripleEquals with Eventually with EitherValues {

  "KafkaProducer.deserializeWebsocketTransaction" should {
    "deserialize a valid String to a WebsocketTransaction" in {
      val str = """{"amount": 0.045318270000000001, "buy_order_id": 1969499130, "sell_order_id": 1969495276, "amount_str": "0.04531827", "price_str": "6339.73", "timestamp": "1533797395", "price": 6339.7299999999996, "type": 0, "id": 71826763}"""
      KafkaProducer.deserializeWebsocketTransaction(str) should ===(SampleWebsocketTransaction)
    }
  }

  "KafkaProducer.convertTransaction" should {
    "convert a WebSocketTransaction to a Transaction" in {
      KafkaProducer.convertWsTransaction(SampleWebsocketTransaction) should ===(SampleTransaction)
    }
  }

  "KafkaProducer.serializeTransaction" should {
    "serialize a Transaction to a String" in {
      KafkaProducer.serializeTransaction(SampleTransaction) should ===(SampleJsonTransaction)
    }
  }

  // TODO test subscribe with a mocked pusher

  // TODO use EmbeddedKafka to test the production / consumption ?
}

object KafkaProducerSpec {
  val SampleWebsocketTransaction = WebsocketTransaction(
    amount = 0.04531827,
    buy_order_id = 1969499130,
    sell_order_id = 1969495276,
    amount_str = "0.04531827",
    price_str = "6339.73",
    timestamp = "1533797395",
    price = 6339.73,
    `type` = 0,
    id = 71826763)

  val SampleTransaction = Transaction(
    date = new Timestamp(1533797395000L),
    tid = 71826763,
    price = 6339.73,
    sell = false,
    amount = 0.04531827)

  val SampleJsonTransaction = """{"date":"2018-08-09 06:49:55","tid":71826763,"price":6339.73,"sell":false,"amount":0.04531827}"""

}
