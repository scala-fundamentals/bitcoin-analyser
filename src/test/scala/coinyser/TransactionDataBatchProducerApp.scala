package coinyser

import java.net.URL
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.time.{LocalDate, OffsetDateTime, ZoneOffset}

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.annotation.tailrec
import scala.io.Source

object TransactionDataBatchProducerApp extends App {
  // In prod, should be a distributed filesystem
  val checkpointDir = "/tmp/coinyser/TransactionDataProducerApp"
  implicit val appConfig: AppConfig = AppConfig(
    topic = "transaction_btcusd",
    bootstrapServers = "localhost:9092",
    checkpointLocation = checkpointDir,
    transactionStorePath = "/tmp/coinyser/transaction"
  )


  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("coinyser")
    .getOrCreate()

  val now = OffsetDateTime.now(ZoneOffset.UTC)
  val start = now.truncatedTo(ChronoUnit.DAYS)
  val end: OffsetDateTime = now.truncatedTo(ChronoUnit.MINUTES)
  val transactions: Dataset[Transaction] = TransactionDataBatchProducer.readTransactions(
    Source.fromURL(new URL("https://www.bitstamp.net/api/v2/transactions/btcusd/?time=day")))

//  TransactionDataBatchProducer.save(transactions, start, end)

  // TODO in Zeppelin:
  // val ds = spark.read.parquet("/tmp/coinyser/transaction/2018-07-26")
  // ds.groupBy(window($"date", "1 hour").as("w")).agg(count($"tid")).sort($"w").show(100,false)

  import spark.implicits._
  appendLastMinuteTransactions(spark.emptyDataset[Transaction])

  @tailrec
  def appendLastMinuteTransactions(previousMinuteTransactions: Dataset[Transaction]): Unit = {
    Thread.sleep(50000)
    val now = OffsetDateTime.now(ZoneOffset.UTC)
    val start = now.truncatedTo(ChronoUnit.MINUTES)
    val end = now.plusMinutes(1).truncatedTo(ChronoUnit.MINUTES)
    val lastMinuteTransactions = TransactionDataBatchProducer.readTransactions(
      Source.fromURL(new URL("https://www.bitstamp.net/api/v2/transactions/btcusd/?time=minute")))

    val transactionsToSave = (previousMinuteTransactions union lastMinuteTransactions).distinct()
//    TransactionDataBatchProducer.save(transactionsToSave, start, end)
    appendLastMinuteTransactions(lastMinuteTransactions)
  }
}
