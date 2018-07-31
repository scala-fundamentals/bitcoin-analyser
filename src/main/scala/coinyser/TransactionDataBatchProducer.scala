package coinyser

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

import cats.Monad
import cats.effect.{IO, Timer}
import org.apache.spark.sql.functions.{explode, from_json, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.concurrent.duration._
import cats.implicits._

class AppContext(implicit val config: AppConfig,
                 implicit val spark: SparkSession,
                 implicit val timer: Timer[IO])

object TransactionDataBatchProducer {
  /** Maximum time required to read transactions from the API */
  val MaxReadTime: FiniteDuration = 5.seconds

  def processRepeatedly(intervalSeconds: Int, jsonTxs: IO[String])
                       (implicit appContext: AppContext): IO[Unit] = {
    import appContext._

    for {
      prevTxs <- readTransactions(jsonTxs)
      prevEndInstant <- currentInstant
      _ <- Monad[IO].tailRecM((prevTxs, prevEndInstant)) {
        case (txs, instant) => processOneBatch(jsonTxs, txs, instant).map(_.asLeft)
      }
    } yield ()
  }

  def processOneBatch(jsonTxs: IO[String], previousTransactions: Dataset[Transaction], previousEnd: Instant)(implicit appCtx: AppContext): IO[(Dataset[Transaction], Instant)] = {
    import appCtx._
    for {
      _ <- IO.sleep(config.intervalBetweenReads - MaxReadTime)

      start = truncateInstant(previousEnd, config.intervalBetweenReads)
      // We are sure that lastTransactions contain all transaction until beforeRead
      beforeRead <- currentInstant
      lastTransactions <- TransactionDataBatchProducer.readTransactions(jsonTxs)
      end = truncateInstant(beforeRead, config.intervalBetweenReads)
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
          require(previousEnd.getEpochSecond < end.getEpochSecond)
          val firstTxs = filterTxs(previousTransactions, start, previousEnd)
          val tailTxs = filterTxs(lastTransactions, previousEnd, end)
          TransactionDataBatchProducer.save(firstTxs union tailTxs, start)
        }

    } yield (lastTransactions, beforeRead)
  }


  def currentInstant(implicit timer: Timer[IO]): IO[Instant] =
    timer.clockRealTime(TimeUnit.SECONDS) map Instant.ofEpochSecond

  // Truncates to the start of interval
  def truncateInstant(instant: Instant, interval: FiniteDuration): Instant = {
    Instant.ofEpochSecond(instant.getEpochSecond / interval.toSeconds * interval.toSeconds)

  }

  def readTransactions(jsonTxs: IO[String])(implicit spark: SparkSession): IO[Dataset[Transaction]] = {
    import spark.implicits._
    val txSchema = Seq.empty[BitstampTransaction].toDS().schema
    val schema = ArrayType(txSchema)
    jsonTxs.map(json =>
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
