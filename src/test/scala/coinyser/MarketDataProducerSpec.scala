package coinyser

import java.sql.Timestamp

import coinyser.MarketDataProducerSpec.{createSource, processData}
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

  "MarketDataProducer.tickerStream" should {
    "create a stream of Ticker data from a constant Source with unique values" in {
      import spark.implicits._

      val stream = MarketDataProducer.tickerStream(createSource())
      val data = processData(stream)
      val ticker = Ticker(
        high = 6821.00,
        last = 6737.59,
        timestamp = new Timestamp(1531056476000L),
        bid = 6737.65
      )
      data should ===(Seq(ticker))
    }

    "fail if the json payload is incorrect" in pending
  }
}

object MarketDataProducerSpec {
  // In companion object to avoid TaskNotSerializable
  def createSource(): Source = Source.fromURL(getClass.getClassLoader.getResource("ticker_btcusd_20180708_1531056459.json"))

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
