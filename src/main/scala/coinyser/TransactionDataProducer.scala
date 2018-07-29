package coinyser

import java.io.File
import java.net.URL
import java.time._
import java.time.temporal.ChronoUnit
import java.util.Properties

import scala.collection.JavaConversions._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._

import scala.io.Source

case class AppConfig(topic: String,
                     bootstrapServers: String,
                     checkpointLocation: String,
                     transactionStorePath: String)

object TransactionDataBatchProducer {
  // TODO ideally use a mocked Clock
  def readSaveRepeatedly(intervalSeconds: Int, createSource: => Source)
                        (implicit appConfig: AppConfig, spark: SparkSession) = {
    def loop(prevTxs: Dataset[Transaction], prevEndInstant: Instant): Unit = {
      Thread.sleep((intervalSeconds - 5) * 1000)

      val start = truncateInstant(prevEndInstant, intervalSeconds)
      val firstTxs = filterTxs(prevTxs, start, prevEndInstant)
      // We are sure that lastTransactions contain all transaction until endInstant
      val endInstant = Instant.now()
      val lastTransactions = TransactionDataBatchProducer.readTransactions(createSource)
      val end = truncateInstant(Instant.now(), intervalSeconds)
      println("start         : " + start)
      println("prevEndInstant: " + prevEndInstant)
      println("endInstant    : " + endInstant)
      println("end           : " + end)
      if (start == end)
        loop(lastTransactions, endInstant)
      else {
        require(prevEndInstant.getEpochSecond < end.getEpochSecond)

        val tailTxs = filterTxs(lastTransactions, prevEndInstant, end)
        TransactionDataBatchProducer.save(firstTxs union tailTxs, start)
        loop(lastTransactions, endInstant)
      }
    }

    // Not referentially transparent !! If you inline prevTxs, that won't work
    val prevTxs = readTransactions(createSource)
    val now = Instant.now()
    loop(prevTxs, now)
  }

  // Truncates to the start of interval
  def truncateInstant(instant: Instant, intervalSeconds: Int): Instant = {
    Instant.ofEpochSecond(instant.getEpochSecond / intervalSeconds * intervalSeconds)

  }

  def readTransactions(createSource: => Source)(implicit spark: SparkSession): Dataset[Transaction] = {
    import spark.implicits._
    val txSchema = Seq.empty[BitstampTransaction].toDS().schema
    val schema = ArrayType(txSchema)
    Seq(createSource.mkString).toDS()
      .select(explode(from_json($"value".cast(StringType), schema)).alias("v"))
      .select(
        $"v.date".cast(LongType).cast(TimestampType).as("date"),
        $"v.tid".cast(IntegerType),
        $"v.price".cast(DoubleType),
        $"v.type".cast(BooleanType).as("sell"),
        $"v.amount".cast(DoubleType))
      .as[Transaction]
  }

  def filterTxs(transactions: Dataset[Transaction], fromInstant: Instant, untilInstant: Instant)
  : Dataset[Transaction] = {
    import transactions.sparkSession.implicits._
    transactions.filter(
      ($"date" >= lit(fromInstant.getEpochSecond).cast(TimestampType)) &&
        ($"date" < lit(untilInstant.getEpochSecond).cast(TimestampType)))
  }

  def save(transactions: Dataset[Transaction], startInstant: Instant)
          (implicit appConfig: AppConfig): String = {
    // TODO logger
    println(s"Saving ${transactions.count()}")

    import transactions.sparkSession.implicits._
    val path = appConfig.transactionStorePath + "/" + OffsetDateTime.ofInstant(startInstant, ZoneOffset.UTC).toLocalDate
    transactions
      .write
      .mode(SaveMode.Append)
      .parquet(path)
    path
  }

}


object TransactionDataProducer {


  def start(url: URL)(implicit kafkaConfig: AppConfig, spark: SparkSession) = {
    val tickerStream = transactionReadStream(_ => Source.fromURL(url))
    kafkaWriteStream(tickerStream)
    //    consoleWriteStream(tickerStream)
  }

  def consoleWriteStream[A](tickerStream: Dataset[A])
                           (implicit spark: SparkSession): StreamingQuery = {
    tickerStream
      .toJSON
      .writeStream
      .format("console")
      .option("truncate", "false")
      .start()
  }


  def kafkaWriteStream[A](tickerStream: Dataset[A])
                         (implicit kafkaConfig: AppConfig, spark: SparkSession): StreamingQuery = {
    tickerStream
      .toJSON
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
      .option("topic", kafkaConfig.topic)
      .option("checkpointLocation", kafkaConfig.checkpointLocation)
      .start()
  }


  def transactionReadStream(createSource: Long => Source)(implicit spark: SparkSession): Dataset[Transaction] = {
    import spark.implicits._
    val txSchema = Seq.empty[BitstampTransaction].toDS().schema
    val schema = ArrayType(txSchema)
    spark.readStream
      .format("rate")
      .load()
      .filter(pmod($"value", lit(5)) === 0)
      .map(row => createSource(row.getAs[Long]("value")).mkString)
      .select(explode(from_json($"value".cast(StringType), schema)).alias("v"))
      .select(
        $"v.date".cast(LongType).cast(TimestampType).as("date"),
        $"v.tid".cast(IntegerType),
        $"v.price".cast(DoubleType),
        $"v.type".cast(BooleanType).as("sell"),
        $"v.amount".cast(DoubleType))
      .withWatermark("date", "20 second")
      .distinct() // we need to use watermark to use distinct, otherwise it will keep everything in memory
      .as[Transaction]
  }


}


