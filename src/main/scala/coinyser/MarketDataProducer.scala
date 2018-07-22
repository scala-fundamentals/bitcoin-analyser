package coinyser

import java.io.File
import java.net.URL
import java.util.Properties

import scala.collection.JavaConversions._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{DoubleType, LongType, TimestampType}

import scala.io.Source

case class KafkaConfig(topic: String,
                       bootstrapServers: String,
                       checkpointLocation: String)

object MarketDataProducer {


  def start(url: URL)(implicit kafkaConfig: KafkaConfig, spark: SparkSession) = {
    val tickerStream = tickerReadStream(_ => Source.fromURL(url))
    kafkaWriteStream(tickerStream)
  }

  def kafkaWriteStream(tickerStream: Dataset[Ticker])
                      (implicit kafkaConfig: KafkaConfig, spark: SparkSession): StreamingQuery = {
    tickerStream
      .toJSON
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
      .option("topic", kafkaConfig.topic)
      .option("checkpointLocation", kafkaConfig.checkpointLocation)
      .start()
  }


  def tickerReadStream(createSource: Long => Source)(implicit spark: SparkSession): Dataset[Ticker] = {
    import spark.implicits._
    val schema = Seq.empty[TickerJson].toDS().schema
    spark.readStream.format("rate")
      .load()
      .map(row => createSource(row.getAs[Long]("value")).mkString)
      .select(from_json($"value".cast("string"), schema, Map("mode" -> "FAILFAST")).alias("v"))
      .select(
        $"v.timestamp".cast(LongType).cast(TimestampType).as("timestamp"),
        $"v.last".cast(DoubleType),
        $"v.bid".cast(DoubleType),
        $"v.ask".cast(DoubleType),
        $"v.vwap".cast(DoubleType),
        $"v.volume".cast(DoubleType))
      .withWatermark("timestamp", "3 second")
      .distinct() // we need to use watermark to use distinct, otherwise it will keep everything in memory
      .as[Ticker]
  }


}


