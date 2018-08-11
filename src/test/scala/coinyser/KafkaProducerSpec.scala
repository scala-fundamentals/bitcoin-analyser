package coinyser

import java.sql.Timestamp

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.{BeforeAndAfterAll, EitherValues, Matchers, WordSpec}
import org.scalatest.concurrent.Eventually

class KafkaProducerSpec extends WordSpec with Matchers with BeforeAndAfterAll with TypeCheckedTripleEquals with Eventually with EitherValues {
  val websocketTransaction = WebsocketTransaction(
    amount = 0.04531827,
    buy_order_id = 1969499130,
    sell_order_id = 1969495276,
    amount_str = "0.04531827",
    price_str = "6339.73",
    timestamp = "1533797395",
    price = 6339.73,
    `type` = 0,
    id = 71826763)

  val transaction = Transaction(
    date = new Timestamp(1533797395000L),
    tid = 71826763,
    price = 6339.73,
    sell = false,
    amount = 0.04531827)

  "KafkaProducer.deserializeWebsocketTransaction" should {
    "deserialize a valid String to a WebsocketTransaction" in {
      val str = """{"amount": 0.045318270000000001, "buy_order_id": 1969499130, "sell_order_id": 1969495276, "amount_str": "0.04531827", "price_str": "6339.73", "timestamp": "1533797395", "price": 6339.7299999999996, "type": 0, "id": 71826763}"""
      KafkaProducer.deserializeWebsocketTransaction(str) should ===(websocketTransaction)
    }
  }

  "KafkaProducer.convertTransaction" should {
    "convert a WebSocketTransaction to a Transaction" in {
      KafkaProducer.convertWsTransaction(websocketTransaction) should ===(transaction)
    }
  }

  "KafkaProducer.serializeTransaction" should {
    "serialize a Transaction to a String" in {
      KafkaProducer.serializeTransaction(transaction) should ===(
        """{"date":1533797395000,"tid":71826763,"price":6339.73,"sell":false,"amount":0.04531827}""")
    }
  }


  // TODO test Spark from_json to_json in a KafkaConsumer: there is a * 1000 somewhere


}
