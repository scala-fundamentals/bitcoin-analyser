package coinyser

import java.sql.Timestamp

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


case class Transaction(date: Timestamp,
                       tid: Int,
                       price: Double,
                       sell: Boolean,
                       amount: Double)
