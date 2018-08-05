package coinyser

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp
import java.time.{Instant, OffsetDateTime}
import java.util.concurrent.TimeUnit

import cats.effect.{IO, Timer}
import coinyser.TransactionDataProducerSpec._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{count, window}
import org.apache.spark.sql.streaming.OutputMode
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, EitherValues, Matchers, WordSpec}

import scala.concurrent.duration._
import scala.io.Source
import TransactionDataBatchProducerSpec.parseTransaction

class TransactionDataBatchProducerSpec extends WordSpec with Matchers with BeforeAndAfterAll with TypeCheckedTripleEquals with Eventually with EitherValues {

  override implicit def patienceConfig: PatienceConfig = new PatienceConfig(10.seconds, 100.millis)

  sys.props("user.timezone") = "UTC"

  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("coinyser")
    .getOrCreate()

  val checkpointDir: File = Files.createTempDirectory("TransactionDataProducerSpec_checkpoint").toFile
  val transactionStoreDir: File = Files.createTempDirectory("TransactionDataBatchProducerSpec_transactions").toFile

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(checkpointDir)
    //    FileUtils.deleteDirectory(transactionStoreDir)
  }

  implicit val appConfig: AppConfig = AppConfig(
    topic = "transaction_btcusd",
    bootstrapServers = "localhost:9092",
    checkpointLocation = checkpointDir.toString,
    transactionStorePath = transactionStoreDir.toString,
    intervalBetweenReads = 1.minute
  )

  implicit object FakeTimer extends Timer[IO] {
    var clockRealTimeInMillis = 0L

    def clockRealTime(unit: TimeUnit): IO[Long] =
      IO(unit.convert(clockRealTimeInMillis, TimeUnit.MILLISECONDS))

    def clockMonotonic(unit: TimeUnit): IO[Long] = ???

    def sleep(duration: FiniteDuration): IO[Unit] = IO {
      clockRealTimeInMillis = clockRealTimeInMillis + duration.toMillis
    }

    def shift: IO[Unit] = ???
  }


  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val appContext: AppContext = new AppContext

  import spark.implicits._


  val transaction1 = Transaction(date = new Timestamp(1532365695000L), tid = 70683282, price = 7740.00, sell = false, amount = 0.10041719)
  val transaction2 = Transaction(date = new Timestamp(1532365693000L), tid = 70683281, price = 7739.99, sell = false, amount = 0.00148564)

  "TransactionDataBatchProducer.readTransactions" should {
    "create a Dataset[Transaction] from a Json String" in {
      val txIO = IO(
        """[{"date": "1532365695", "tid": "70683282", "price": "7740.00", "type": "0", "amount": "0.10041719"},
          |{"date": "1532365693", "tid": "70683281", "price": "7739.99", "type": "0", "amount": "0.00148564"}]""".stripMargin)

      val ds: Dataset[Transaction] = TransactionDataBatchProducer.readTransactions(txIO).unsafeRunSync()
      val data = ds.collect()
      data should contain theSameElementsAs Seq(transaction1, transaction2)
    }

    "fail if the json payload is incorrect" in pending
  }

  "TransactionDataBatchProducer.processOneBatch" should {
    "Wait a bit of time, fetch the next batch of transactions, and save a filtered union of the previous and the last batch" in {
      // TODO timezone
      val transactions = Seq(
        "|2018-08-02 07:22:34|71319732|7657.58|true |0.021762  |",
        "|2018-08-02 07:22:47|71319735|7663.85|false|0.01385517|",
        "|2018-08-02 07:23:09|71319738|7663.85|false|0.03782426|",
        "|2018-08-02 07:23:11|71319739|7663.86|false|0.15750809|",
        "|2018-08-02 07:23:40|71319751|7661.49|true |0.1       |",
        "|2018-08-02 07:23:41|71319752|7661.49|true |0.04437627|",
        "|2018-08-02 07:23:41|71319753|7661.49|true |0.05562373|",
        "|2018-08-02 07:23:41|71319754|7661.49|true |0.0160586 |",
        "|2018-08-02 07:23:44|71319755|7661.48|false|0.1799    |",
        "|2018-08-02 07:24:04|71319758|7661.46|true |0.012848  |",
        "|2018-08-02 07:24:04|71319760|7661.46|false|0.01852   |",
        "|2018-08-02 07:24:05|71319761|7657.58|true |0.028632  |",
        "|2018-08-02 07:24:42|71319773|7661.47|false|0.017882  |",
        "|2018-08-02 07:24:45|71319774|7662.68|false|0.016105  |",
        "|2018-08-02 07:24:45|71319775|7663.85|false|0.03149464|",
        "|2018-08-02 07:24:46|71319776|7663.85|false|0.04029315|",
        "|2018-08-02 07:24:50|71319779|7663.85|true |0.03602883|",
        "|2018-08-02 07:24:50|71319780|7663.86|false|0.0777    |",
        "|2018-08-02 07:25:08|71319782|7663.85|true |0.00181743|",
        "|2018-08-02 07:25:14|71319783|7663.85|true |0.04211058|",
        "|2018-08-02 07:25:14|71319784|7663.85|true |0.01700019|",
        "|2018-08-02 07:25:15|71319785|7663.85|false|0.00951691|",
        "|2018-08-02 07:25:45|71319789|7661.68|true |0.0076989 |",
        "|2018-08-02 07:25:51|71319793|7661.69|false|0.02855881|",
        "|2018-08-02 07:25:51|71319794|7661.68|true |0.04980948|",
        "|2018-08-02 07:25:52|71319795|7661.68|true |0.01378989|",
        "|2018-08-02 07:26:17|71319799|7661.38|false|0.01690441|",
        "|2018-08-02 07:26:18|71319800|7661.38|true |0.1       |",
        "|2018-08-02 07:26:19|71319801|7663.43|false|0.1       |",
        "|2018-08-02 07:26:19|71319802|7663.86|false|0.12409542|"
      ).map(parseTransaction)

      val txs0 = transactions.filter(tx => Set(71319739, 71319738, 71319735, 71319732).contains(tx.tid))
      val txs1 = transactions.filter(tx => Set(71319760, 71319751, 71319752, 71319755, 71319761, 71319754, 71319758, 71319753).contains(tx.tid))
      val txs2 = transactions.filter(tx => Set(71319773, 71319779, 71319776, 71319780, 71319775, 71319774).contains(tx.tid))
      val txs3 = transactions.filter(tx => Set(71319783, 71319784, 71319793, 71319794, 71319785, 71319782, 71319795, 71319789).contains(tx.tid))
      val expectedTxs = transactions.filter(tx => Set(71319738, 71319739, 71319751, 71319752, 71319753, 71319754, 71319755, 71319758, 71319760, 71319761, 71319773, 71319774, 71319775, 71319776, 71319779, 71319780).contains(tx.tid))

      FakeTimer.clockRealTimeInMillis = Instant.parse("2018-08-02T06:23:32Z").toEpochMilli
      val ((ds1, instant1), (ds2, instant2), (ds3, instant3)) = {
        for {
          tuple1 <- TransactionDataBatchProducer.processOneBatch(
            IO(txs1.toDS()),
            txs0.toDS(),
            Instant.parse("2018-08-02T06:23:00Z"),
            Instant.parse("2018-08-02T06:23:26Z"))

          tuple2 <- TransactionDataBatchProducer.processOneBatch(
            IO(txs2.toDS()),
            tuple1._1,
            TransactionDataBatchProducer.truncateInstant(tuple1._2, 1.minute),
            tuple1._2)

          tuple3 <- TransactionDataBatchProducer.processOneBatch(
            IO(txs3.toDS()),
            tuple2._1,
            TransactionDataBatchProducer.truncateInstant(tuple2._2, 1.minute),
            tuple2._2)
        } yield (tuple1, tuple2, tuple3)
      }.unsafeRunSync()

      val savedTransactions = spark.read.parquet(appConfig.transactionStorePath).as[Transaction].collect()
      savedTransactions.map(_.tid).toSet should ===(expectedTxs.map(_.tid).toSet)
      // TODO check tuple1, tuple2, tuple3
    }


    // TODO improve this test to highlight the scenario above
    "TransactionDataBatchProducer.readSaveRepeatedly" should {
      "fetch new transactions every 10s and save them" in {
        def txIO = IO {
          // TODO use IO clock
          val now = OffsetDateTime.now().toEpochSecond
          val transactions = Seq.tabulate(10)(i =>
            s"""{"date": "${now - i}", "tid": "${now - i}", "price": "7740.00", "type": "0", "amount": "0.10041719"}"""
          )
          "[" + transactions.mkString(",") + "]"
        }

        val intervalSeconds = 10
        val io = for {
          _ <- IO.shift
          _ <- TransactionDataBatchProducer.processRepeatedly(txIO, txIO)
        } yield ()
        io.unsafeRunTimed(35.seconds)

        val savedTransactions = spark.read.parquet(appConfig.transactionStorePath).as[Transaction]
        val counts = savedTransactions
          .groupBy(window($"date", "10 seconds").as("window"))
          .agg(count($"tid").as("count"))
          .sort($"window")
        counts.show(10000, false)
        counts.select($"count".as[Long]).collect().toSeq should ===(Seq.fill(3)(intervalSeconds.toLong))
      }
    }

    // TODO test for partition dt


  }


}

object TransactionDataBatchProducerSpec {
  def parseTransaction(s: String): Transaction =
    s.split('|').toList match {
      case _ +: date +: tid +: amount +: sell +: price +: Nil =>
        Transaction(Timestamp.valueOf(date), tid.toInt, price.toDouble, sell.trim.toBoolean, amount.toDouble)
    }

}
