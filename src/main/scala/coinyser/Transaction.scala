package coinyser

import java.sql.Timestamp

case class BitstampTransaction(date: String,
                               tid: String,
                               price: String,
                               `type`: String,
                               amount: String)

case class Transaction(date: Timestamp,
                       tid: Int,
                       price: Double,
                       sell: Boolean,
                       amount: Double)
