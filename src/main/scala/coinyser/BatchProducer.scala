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
import coinyser.AppConfig

class AppContext(implicit val config: AppConfig,
                 implicit val spark: SparkSession,
                 implicit val timer: Timer[IO])

object BatchProducer {
  /** Maximum time required to read transactions from the API */
  val MaxReadTime: FiniteDuration = 15.seconds
  /** Number of seconds required by the API to make a transaction visible */
  val ApiLag: FiniteDuration = 5.seconds


  def processRepeatedly(initialJsonTxs: IO[String], jsonTxs: IO[String])
                       (implicit appContext: AppContext): IO[Unit] = {
    import appContext._

    for {
      firstTxs <- readTransactions(initialJsonTxs)
      firstEnd <- currentInstant
      firstStart = truncateInstant(firstEnd, config.firstInterval)
      _ <- Monad[IO].tailRecM((firstTxs, firstStart, firstEnd)) {
        case (txs, start, instant) =>
          processOneBatch(readTransactions(jsonTxs), txs, start, instant).map(_.asLeft)
      }
    } yield ()
  }

  def processOneBatch(lastTransactionsIO: IO[Dataset[Transaction]],
                      previousTransactions: Dataset[Transaction],
                      batchStart: Instant,
                      previousEnd: Instant)(implicit appCtx: AppContext)
  : IO[(Dataset[Transaction], Instant, Instant)] = {
    import appCtx._
    import spark.implicits._

    for {
      _ <- IO.sleep(config.intervalBetweenReads - MaxReadTime)

      beforeRead <- currentInstant
      // We are sure that lastTransactions contain all transactions until lastEnd
      end = beforeRead.minusSeconds(ApiLag.toSeconds)
      lastTransactions <- lastTransactionsIO
      batchEnd = truncateInstant(end, config.intervalBetweenReads)
      _ <- IO {
        // TODO use logger
        println("previousEnd: " + previousEnd)
        println("end        : " + end)
        println("beforeRead : " + beforeRead)
        println("batchStart      : " + batchStart)
        println("batchEnd        : " + batchEnd)
        //        println(lastTransactions.map(_.tid).collect().toSet)
      }
      transactions <-
        if (batchStart == batchEnd) {
          IO.pure((previousTransactions union lastTransactions).distinct())
        }
        else {
          require(previousEnd.getEpochSecond < batchEnd.getEpochSecond)
          val firstTxs = filterTxs(previousTransactions, batchStart, previousEnd)
          val tailTxs = filterTxs(lastTransactions, previousEnd, batchEnd)
          BatchProducer.save(firstTxs union tailTxs).map(_ => lastTransactions)
        }

    } yield (transactions, batchEnd, end)
  }


  def currentInstant(implicit timer: Timer[IO]): IO[Instant] =
    timer.clockRealTime(TimeUnit.SECONDS) map Instant.ofEpochSecond

  // Truncates to the start of interval
  def truncateInstant(instant: Instant, interval: FiniteDuration): Instant = {
    Instant.ofEpochSecond(instant.getEpochSecond / interval.toSeconds * interval.toSeconds)

  }

  def readTransactions(jsonTxs: IO[String])(implicit spark: SparkSession): IO[Dataset[Transaction]] = {
    import spark.implicits._
    val txSchema = Seq.empty[HttpTransaction].toDS().schema
    val schema = ArrayType(txSchema)
    jsonTxs.map(json =>
      Seq(json).toDS()
        .select(explode(from_json($"value".cast(StringType), schema)).alias("v"))
        .select(
          $"v.date".cast(LongType).cast(TimestampType).as("timestamp"),
          $"v.date".cast(LongType).cast(TimestampType).cast(DateType).as("date"),
          $"v.tid".cast(IntegerType),
          $"v.price".cast(DoubleType),
          $"v.type".cast(BooleanType).as("sell"),
          $"v.amount".cast(DoubleType))
        .as[Transaction])
  }

  def filterTxs(transactions: Dataset[Transaction], fromInstant: Instant, untilInstant: Instant)
  : Dataset[Transaction] = {
    import transactions.sparkSession.implicits._
    val filtered = transactions.filter(
      ($"timestamp" >= lit(fromInstant.getEpochSecond).cast(TimestampType)) &&
        ($"timestamp" < lit(untilInstant.getEpochSecond).cast(TimestampType)))
    println(s"filtered ${filtered.count()}/${transactions.count()} from $fromInstant until $untilInstant")
    filtered
  }

  def save(transactions: Dataset[Transaction])
          (implicit appConfig: AppConfig): IO[String] = {
    // TODO logger
    println(s"Saving ${transactions.count()}")
    val path = appConfig.transactionStorePath
    IO {
      transactions
        .write
        .mode(SaveMode.Append)
        .partitionBy("date")
        .parquet(path)
      path
    }
  }

}
