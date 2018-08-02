package coinyser

import java.net.URL

import cats.effect.IO
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration._
import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global

object TransactionDataBatchProducerApp extends App {
  // In prod, should be a distributed filesystem
  val checkpointDir = "/tmp/coinyser/TransactionDataProducerApp"
  implicit val appConfig: AppConfig = AppConfig(
    topic = "transaction_btcusd",
    bootstrapServers = "localhost:9092",
    checkpointLocation = checkpointDir,
    transactionStorePath = "/tmp/coinyser/transaction",
    intervalBetweenReads = 1.minute
  )

  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("coinyser")
    .getOrCreate()

  implicit val appContext: AppContext = new AppContext()

  val initialJsonTxs = IO {
    Source.fromURL(new URL("https://www.bitstamp.net/api/v2/transactions/btcusd/?time=minute")).mkString
  }

  val nextJsonTxs = IO {
    Source.fromURL(new URL("https://www.bitstamp.net/api/v2/transactions/btcusd/?time=minute")).mkString
  }
  TransactionDataBatchProducer.processRepeatedly(initialJsonTxs, nextJsonTxs).unsafeRunSync()

  // TODO in Zeppelin:
  // val ds = spark.read.parquet("/tmp/coinyser/transaction/2018-07-26")
  // ds.groupBy(window($"date", "1 hour").as("w")).agg(count($"tid")).sort($"w").show(100,false)
  //  appendLastMinuteTransactions(spark.emptyDataset[Transaction])


}
