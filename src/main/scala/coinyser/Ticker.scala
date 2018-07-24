package coinyser

import java.sql.Timestamp

case class Ticker(timestamp: Timestamp,
                  last: Double,
                  bid: Double,
                  ask: Double,
                  vwap: Double,
                  volume: Double)
