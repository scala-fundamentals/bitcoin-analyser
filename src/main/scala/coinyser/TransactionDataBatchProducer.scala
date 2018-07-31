package coinyser

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

import cats.effect.{IO, Timer}
import org.apache.spark.sql.functions.{explode, from_json, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.concurrent.duration._

object TransactionDataBatchProducer {
  def readSaveRepeatedly(intervalSeconds: Int, jsonIO: IO[String])
                        (implicit appConfig: AppConfig, spark: SparkSession, timer: Timer[IO]): IO[Unit] = {
    def loop(prevTxs: Dataset[Transaction], prevEndInstant: Instant): IO[Unit] = {
      for {
        _ <- IO.sleep((intervalSeconds - 5).seconds)

        start = truncateInstant(prevEndInstant, intervalSeconds)
        firstTxs = filterTxs(prevTxs, start, prevEndInstant)
        // We are sure that lastTransactions contain all transaction until endInstant
        beforeReadInstant <- now
        lastTransactions <- TransactionDataBatchProducer.readTransactions(jsonIO)
        end = truncateInstant(beforeReadInstant, intervalSeconds) // TODO should be beforeReadInstant, we might miss a transaction
//        _ <- IO {
//          println("start             : " + start)
//          println("prevEndInstant    : " + prevEndInstant)
//          println("beforeReadInstant : " + beforeReadInstant)
//          println("end               : " + end)
//        }
        _ <-
          if (start == end)
            loop(lastTransactions, beforeReadInstant)
          else {
            require(prevEndInstant.getEpochSecond < end.getEpochSecond)
            val tailTxs = filterTxs(lastTransactions, prevEndInstant, end)
            TransactionDataBatchProducer.save(firstTxs union tailTxs, start)
            loop(lastTransactions, beforeReadInstant)
          }

      } yield ()
    }

    readTransactions(jsonIO) flatMap { prevTxs =>
      val now = Instant.now()
      loop(prevTxs, now)
    }
  }

  def now(implicit timer: Timer[IO]): IO[Instant] =
    timer.clockRealTime(TimeUnit.SECONDS) map Instant.ofEpochSecond

  // Truncates to the start of interval
  def truncateInstant(instant: Instant, intervalSeconds: Int): Instant = {
    Instant.ofEpochSecond(instant.getEpochSecond / intervalSeconds * intervalSeconds)

  }

  def readTransactions(jsonIO: IO[String])(implicit spark: SparkSession): IO[Dataset[Transaction]] = {
    import spark.implicits._
    val txSchema = Seq.empty[BitstampTransaction].toDS().schema
    val schema = ArrayType(txSchema)
    jsonIO.map(json =>
      Seq(json).toDS()
        .select(explode(from_json($"value".cast(StringType), schema)).alias("v"))
        .select(
          $"v.date".cast(LongType).cast(TimestampType).as("date"),
          $"v.tid".cast(IntegerType),
          $"v.price".cast(DoubleType),
          $"v.type".cast(BooleanType).as("sell"),
          $"v.amount".cast(DoubleType))
        .as[Transaction])
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
    val path = appConfig.transactionStorePath + "/" + OffsetDateTime.ofInstant(startInstant, ZoneOffset.UTC).toLocalDate
    transactions
      .write
      .mode(SaveMode.Append)
      .parquet(path)
    path
  }

}
