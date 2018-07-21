package coinyser

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryProgress}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.duration._
import scala.io.Source
import MarketDataProducerSpec._

class MarketDataProducerSpec extends WordSpec with Matchers with BeforeAndAfterAll with TypeCheckedTripleEquals with Eventually {

  override implicit def patienceConfig: PatienceConfig = new PatienceConfig(10.seconds, 100.millis)

  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("coinyser")
    .getOrCreate()

  val checkpointDir: File = Files.createTempDirectory("MarketDataProducerSpec_checkpoint").toFile

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(checkpointDir)
  }

  implicit val kafkaConfig: KafkaConfig = KafkaConfig(
    topic = "ticker_btcusd",
    bootstrapServers = "localhost:9092",
    checkpointLocation = checkpointDir.toString)

  import spark.implicits._

  val ticker1 = Ticker(
    last = 6737.45,
    timestamp = new Timestamp(1531056456000L),
    bid = 6737.45,
    ask = 6740.00,
    vwap = 6692.42,
    volume = 4821.03915289)

  val ticker2 = Ticker(
    last = 6737.59,
    timestamp = new Timestamp(1531056476000L),
    bid = 6737.65,
    ask = 6739.99,
    vwap = 6692.51,
    volume = 4818.42662236
  )

  "MarketDataProducer.tickerReadStream" should {
    "create a stream of Ticker data from a constant Source with unique values" in {
      val stream = MarketDataProducer.tickerReadStream(createConstantSource)
      val data = processReadStream(stream, 4)
      data should ===(Seq(ticker1))
    }

    "keep tickers if they arrive out of order within the watermark period" in {
      val stream = MarketDataProducer.tickerReadStream(createOutOfOrderSource)
      val data = processReadStream(stream, 4)
      data.map(_.timestamp.getTime).toSet should ===(Set(1531056476000L, 1531056475000L, 1531056474000L))
    }

    "fail if the json payload is incorrect" in pending
  }

  "MarketDataProducer.kafkaWriteStream" should {
    "write a stream of Ticker to Kafka" in {
      implicit val sqlc: SQLContext = spark.sqlContext
      val tickerStream = MemoryStream[Ticker]
      tickerStream.addData(ticker1, ticker2)

      val kafkaStream = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
        .option("subscribe", kafkaConfig.topic)
        .load()
        .select($"value")
        .as[String]

      val streamingQuery = MarketDataProducer.kafkaWriteStream(tickerStream.toDS())
      // Order is not guaranteed: each mini-batch is processed in parallel
      processReadStream(kafkaStream, 2).toSet should ===(Set(
        """{"timestamp":"2018-07-08T15:27:56.000+02:00","last":6737.59,"bid":6737.65,"ask":6739.99,"vwap":6692.51,"volume":4818.42662236}""",
        """{"timestamp":"2018-07-08T15:27:36.000+02:00","last":6737.45,"bid":6737.45,"ask":6740.0,"vwap":6692.42,"volume":4821.03915289}"""))

      streamingQuery.stop()
    }

  }

  def processReadStream[A: Encoder](stream: Dataset[A], expectedTotalInputRows: Int)(implicit spark: SparkSession): Seq[A] = {
    val query = stream
      .writeStream
      .format("memory")
      .queryName("Output")
      .outputMode(OutputMode.Append())
      .start()

    eventually {
      val totalRows = query.recentProgress.map(_.numInputRows).sum
      //println(query.recentProgress.map(p => (p.batchId, p.numInputRows)).mkString(", "))
      totalRows.toInt should be >= expectedTotalInputRows
    }
    query.stop()
    val result = spark.sql("select * from Output").as[A].collect().toSeq
    result
  }

}

object MarketDataProducerSpec {

  // In companion object to avoid TaskNotSerializable

  def createConstantSource(msgCount: Long): Source =
    Source.fromURL(getClass.getClassLoader.getResource("ticker_btcusd_20180708_1531056456.json"))


  def createOutOfOrderSource(msgCount: Long): Source =
    Source.fromString(
      s"""{
         |"high": "6821.00",
         |"last": "6737.59",
         |"timestamp": "${1531056476 - msgCount}",
         |"bid": "6737.65",
         |"vwap": "6692.51",
         |"volume": "4818.42662236",
         |"low": "6510.00",
         |"ask": "6739.99",
         |"open": "6755.46"
         |}""".stripMargin)


}
