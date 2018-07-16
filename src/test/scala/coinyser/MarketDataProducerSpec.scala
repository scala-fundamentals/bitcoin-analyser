package coinyser

import java.sql.Timestamp

import coinyser.MarketDataProducerSpec.{createConstantSource, createOutOfOrderSource, processData}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql._
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.{Matchers, WordSpec}

import scala.io.Source

class MarketDataProducerSpec extends WordSpec with Matchers with TypeCheckedTripleEquals {
  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("coinyser")
    .getOrCreate()

  import spark.implicits._


  "MarketDataProducer.tickerStream" should {
    "create a stream of Ticker data from a constant Source with unique values" in {
      val stream = MarketDataProducer.tickerStream(createConstantSource)
      val data = processData(stream)
      val ticker = Ticker(
        high = 6821.00,
        last = 6737.59,
        timestamp = new Timestamp(1531056476000L),
        bid = 6737.65
      )
      data should ===(Seq(ticker))
    }

    "keep tickers if they arrive out of order within the watermark period" in {
      val stream = MarketDataProducer.tickerStream(createOutOfOrderSource)
      val data = processData(stream)
      data.map(_.timestamp.getTime) should ===(Seq(1531056476000L, 1531056475000L, 1531056474000L))
    }

    "fail if the json payload is incorrect" in pending
  }
}

object MarketDataProducerSpec {
  // In companion object to avoid TaskNotSerializable
  def createConstantSource(msgCount: Long): Source =
    Source.fromURL(getClass.getClassLoader.getResource("ticker_btcusd_20180708_1531056459.json"))

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

  def processData[A: Encoder](stream: Dataset[A])(implicit spark: SparkSession): Seq[A] = {
    val query = stream
      .writeStream
      .format("memory")
      .queryName("Output")
      .outputMode(OutputMode.Append())
      .start()

    Thread.sleep(5000)
    val result = spark.sql("select * from Output").as[A].collect().toSeq
    query.stop()
    result
  }
}
