package coinyser

import java.sql.Timestamp

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.streaming.StreamingContext

case class TickerJson(high: String,
                      last: String,
                      timestamp: String,
                      bid: String,
                      vwap: String,
                      volume: String,
                      low: String,
                      ask: String,
                      open: String)

case class Ticker(high: Double,
                  last: Double,
                  timestamp: Timestamp,
                  bid: Double)/*,
                  vwap: String,
                  volume: String,
                  low: String,
                  ask: String,
                  open: String)*/


object BatchConsumer {
  def start(implicit spark: SparkSession): StreamingQuery = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val schema = Seq.empty[TickerJson].toDS().schema

    val inStream: Dataset[TickerJson] = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "ticker_btcusd")
      .load()
      .select(from_json(col("value").cast("string"), schema).alias("v"))
      .select("v.*").as[TickerJson]

    inStream.writeStream.format("console").start()
    //        spark.read.json(ds)
  }

}


