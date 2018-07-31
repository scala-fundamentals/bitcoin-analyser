package coinyser

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

import cats.Monad
import cats.effect.{IO, Timer}
import org.apache.spark.sql.functions.{explode, from_json, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.annotation.tailrec
import scala.concurrent.duration._
import cats.implicits._

object TransactionDataBatchProducer {
  /** Maximum time required to read transactions from the API */
  val MaxReadTime = 5

  def readSaveRepeatedly(intervalSeconds: Int, jsonIO: IO[String])
                        (implicit appConfig: AppConfig, spark: SparkSession, timer: Timer[IO]): IO[Unit] = {
    def loop(prevTxs: Dataset[Transaction], prevEndInstant: Instant): IO[(Dataset[Transaction], Instant)] = {
      for {
        _ <- IO.sleep((intervalSeconds - MaxReadTime).seconds)

        start = truncateInstant(prevEndInstant, intervalSeconds)
        // We are sure that lastTransactions contain all transaction until beforeRead
        beforeRead <- currentInstant
        lastTransactions <- TransactionDataBatchProducer.readTransactions(jsonIO)
        end = truncateInstant(beforeRead, intervalSeconds)
        //        _ <- IO {
        //          println("start             : " + start)
        //          println("prevEndInstant    : " + prevEndInstant)
        //          println("beforeReadInstant : " + beforeReadInstant)
        //          println("end               : " + end)
        //        }
        savedParquetPath <-
          if (start == end) {
            IO.unit
          }
          else {
            require(prevEndInstant.getEpochSecond < end.getEpochSecond)
            val firstTxs = filterTxs(prevTxs, start, prevEndInstant)
            val tailTxs = filterTxs(lastTransactions, prevEndInstant, end)
            TransactionDataBatchProducer.save(firstTxs union tailTxs, start)
          }

      } yield (lastTransactions, beforeRead)
    }

    for {
      prevTxs <- readTransactions(jsonIO)
      prevEndInstant <- currentInstant
      _ <- (prevTxs, prevEndInstant).tailRecM {
        case (txs, instant) => loop(txs, instant).map(_.asLeft)
      }
    } yield ()
  }

  def currentInstant(implicit timer: Timer[IO]): IO[Instant] =
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
          (implicit appConfig: AppConfig): IO[String] = {
    // TODO logger
    println(s"Saving ${transactions.count()}")
    val path = appConfig.transactionStorePath + "/dt=" + OffsetDateTime.ofInstant(startInstant, ZoneOffset.UTC).toLocalDate
    IO {
      transactions
        .write
        .mode(SaveMode.Append)
        .parquet(path)
      path
    }
  }

}
