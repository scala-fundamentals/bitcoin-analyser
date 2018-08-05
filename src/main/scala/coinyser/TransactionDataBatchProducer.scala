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
  val MaxReadTime: FiniteDuration = 15.seconds
  /** Number of seconds required by the API to make a transaction visible */
  val ApiLag: FiniteDuration = 5.seconds


  def processRepeatedly(initialJsonTxs: IO[String], jsonTxs: IO[String])
                       (implicit appContext: AppContext): IO[Unit] = {
    import appContext._

    for {
      firstTxs <- readTransactions(initialJsonTxs)
      firstEnd <- currentInstant
      firstStart = truncateInstant(firstEnd, 1.day) // TODO parameterize firstInterval
      _ <- Monad[IO].tailRecM((firstTxs, firstStart, firstEnd)) {
        case (txs, start, instant) =>
          processOneBatch(readTransactions(jsonTxs), txs, start, instant).map {
            case (nextTransactions, nextEnd) =>
              val nextStart = truncateInstant(nextEnd, config.intervalBetweenReads)
              Left((nextTransactions, nextStart, nextEnd))
          }
      }
    } yield ()
  }

  // TODO return nextStart, will simplify testing
  def processOneBatch(lastTransactionsIO: IO[Dataset[Transaction]],
                      previousTransactions: Dataset[Transaction],
                      start: Instant, previousEnd: Instant)(implicit appCtx: AppContext)
  : IO[(Dataset[Transaction], Instant)] = {
    import appCtx._
    import spark.implicits._

    for {
      _ <- IO.sleep(config.intervalBetweenReads - MaxReadTime)

      beforeRead <- currentInstant
      // We are sure that lastTransactions contain all transactions until lastEnd
      lastEnd = beforeRead.minusSeconds(ApiLag.toSeconds)
      lastTransactions <- lastTransactionsIO
      end = truncateInstant(lastEnd, config.intervalBetweenReads)
      _ <- IO {
        println("start      : " + start)
        println("previousEnd: " + previousEnd)
        println("lastEnd    : " + lastEnd)
        println("beforeRead : " + beforeRead)
        println("end        : " + end)
        println(lastTransactions.map(_.tid).collect().toSet)
      }
      transactions <-
        if (start == end) {
          IO.pure((previousTransactions union lastTransactions).distinct())
        }
        else {
          require(previousEnd.getEpochSecond < end.getEpochSecond)
          val firstTxs = filterTxs(previousTransactions, start, previousEnd)
          val tailTxs = filterTxs(lastTransactions, previousEnd, end)
          TransactionDataBatchProducer.save(firstTxs union tailTxs, start).map(_ => lastTransactions)
        }

    } yield (transactions, lastEnd)
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
    val filtered = transactions.filter(
      ($"date" >= lit(fromInstant.getEpochSecond).cast(TimestampType)) &&
        ($"date" < lit(untilInstant.getEpochSecond).cast(TimestampType)))
    println(s"filtered ${filtered.count()}/${transactions.count()} from $fromInstant until $untilInstant")
    filtered
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
