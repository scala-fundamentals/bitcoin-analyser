package coinyser

import java.sql.Timestamp
import java.time.ZoneOffset

case class HttpTransaction(date: String,
                           tid: String,
                           price: String,
                           `type`: String,
                           amount: String)

case class WebsocketTransaction(amount: Double,
                                buy_order_id: Long,
                                sell_order_id: Long,
                                amount_str: String,
                                price_str: String,
                                timestamp: String,
                                price: Double,
                                `type`: Int,
                                id: Int)


case class Transaction(timestamp: Timestamp,
                       date: String,
                       tid: Int,
                       price: Double,
                       sell: Boolean,
                       amount: Double) {
}

object Transaction {
  // Use a second constructor so that Spark can use date as a column
  def apply(timestamp: Timestamp,
            tid: Int,
            price: Double,
            sell: Boolean,
            amount: Double) =
    new Transaction(
      timestamp = timestamp,
      date = timestamp.toLocalDateTime.atOffset(ZoneOffset.UTC).toLocalDate.toString,
      tid = tid,
      price = price,
      sell = sell,
      amount = amount)

}
