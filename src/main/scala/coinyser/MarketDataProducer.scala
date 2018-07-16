package coinyser

import java.io.File
import java.net.URL
import java.util.Properties

import scala.collection.JavaConversions._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, TimestampType}

import scala.io.Source

object MarketDataProducer {

  // TODO use Monix Task ?
  // TODO or use Spark streaming here as well ?
  def sendFile(url: URL): Unit = {
    val props: Map[String, Object] = Map(
      ("bootstrap.servers", "localhost:9092"),
      ("acks", "all"),
      ("retries", new Integer(0)),
      ("batch.size", new Integer(16384)),
      ("linger.ms", new Integer(1)),
      ("buffer.memory", new Integer(33554432)),
      ("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
      ("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    )

    // TODO what to put in the partition key? timestamp ? Can it help deduplicating message if we poll too frequently ?
    val producer = new KafkaProducer[String, String](mapAsJavaMap(props))
    val orderBook = Source.fromURL(url).mkString
    val javaFuture = producer.send(new ProducerRecord[String, String]("ticker_btcusd", null, orderBook))
    // TODO what should I do with this future ?
    println("sent !")
    producer.close()
  }

  def start(url: URL)(implicit spark: SparkSession) = {
    import spark.implicits._
    val schema = Seq.empty[TickerJson].toDS().schema
    //    * For a streaming query, you may use the function `current_timestamp` to generate windows on
    val in = spark.readStream.format("rate")
      .load()
      .map(_ => Source.fromURL(url).mkString)
      .select(from_json($"value".cast("string"), schema).alias("v"))
      .select($"v.timestamp".cast(TimestampType), $"v.timestamp".as("ts"), $"v.last", $"v.bid")
      .withWatermark("timestamp", "5 seconds")
      .distinct()
    //      .groupBy(window(current_timestamp(), "5 seconds")).count()

    in.writeStream.format("console").start()


    //    in
    //      .writeStream
    //      .format("kafka")
    //      .option("kafka.bootstrap.servers", "localhost:9092")
    //      .option("topic", "ticker_btcusd")
    //      .option("checkpointLocation", "/tmp/coinyser/checkpoint")
    //      .start()
  }


  def tickerStream(createSource: Long => Source)(implicit spark: SparkSession): Dataset[Ticker] = {
    import spark.implicits._
    val schema = Seq.empty[TickerJson].toDS().schema
    spark.readStream.format("rate")
      .load()
      .map(row => createSource(row.getAs[Long]("value")).mkString)
      .select(from_json($"value".cast("string"), schema, Map("mode" -> "FAILFAST")).alias("v"))
      .select(
        $"v.timestamp".cast(LongType).cast(TimestampType).as("timestamp"),
        $"v.high".cast(DoubleType),
        $"v.last".cast(DoubleType),
        $"v.bid".cast(DoubleType))
      .withWatermark("timestamp", "3 second")
      .distinct()
      .as[Ticker]
  }


}


