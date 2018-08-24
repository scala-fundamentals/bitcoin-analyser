package coinyser

import java.sql.Timestamp
import java.time.ZoneOffset



case class Transaction(timestamp: Timestamp,
                       date: String,
                       tid: Int,
                       price: Double,
                       sell: Boolean,
                       amount: Double)


object Transaction {
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
