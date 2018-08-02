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
      /*
start      : 2018-08-01T10:21:00Z
previousEnd: 2018-08-01T10:21:20Z
lastEnd    : 2018-08-01T10:22:12Z
end        : 2018-08-01T10:22:00Z
Set(71272533, 71272540, 71272521, 71272528, 71272554, 71272536, 71272512, 71272545, 71272557, 71272546, 71272525, 71272522, 71272537, 71272548, 71272538, 71272535, 71272520, 71272529, 71272562, 71272555, 71272526, 71272530, 71272541, 71272516, 71272547, 71272534, 71272539, 71272556, 71272561, 71272527, 71272542, 71272559)
Saving 32
18/08/01 12:22:22 INFO FileOutputCommitter: Saved output of task 'attempt_20180801122222_0044_m_000001_0' to file:/tmp/coinyser/transaction/dt=2018-08-01/_temporary/0/task_20180801122222_0044_m_000001
18/08/01 12:22:22 INFO FileOutputCommitter: Saved output of task 'attempt_20180801122222_0044_m_000000_0' to file:/tmp/coinyser/transaction/dt=2018-08-01/_temporary/0/task_20180801122222_0044_m_000000
start      : 2018-08-01T10:22:00Z
previousEnd: 2018-08-01T10:22:12Z
lastEnd    : 2018-08-01T10:23:07Z
end        : 2018-08-01T10:23:00Z
Set(71272579, 71272582, 71272628, 71272596, 71272614, 71272618, 71272622, 71272557, 71272595, 71272602, 71272623, 71272562, 71272616, 71272555, 71272594, 71272631, 71272603, 71272625, 71272615, 71272556, 71272561, 71272559, 71272630)
Saving 20
       */

      def parseTransaction(s: String): Transaction =
        s.split('|').toList match {
          case _ +: date +: tid +: amount +: sell +: price +: Nil =>
            Transaction(Timestamp.valueOf(date), tid.toInt, price.toDouble, sell.trim.toBoolean, amount.toDouble)
        }

      val transactions1 = Seq(
        // TODO add a few before 2018-08-01T10:22:00Z to have some saving in the first batch
        "|2018-08-01 12:22:04|71272546|7552.38|true |0.07275619|",
        "|2018-08-01 12:22:04|71272547|7552.37|true |0.02724381|",
        "|2018-08-01 12:22:04|71272548|7552.37|true |0.1       |",
        "|2018-08-01 12:22:12|71272554|7552.39|false|0.499052  |",
        "|2018-08-01 12:22:13|71272555|7552.37|true |0.1       |",
        "|2018-08-01 12:22:13|71272556|7552.39|false|0.40094701|",
        "|2018-08-01 12:22:14|71272557|7552.95|false|0.0036    |",
        "|2018-08-01 12:22:14|71272559|7552.95|false|0.119455  |",
        "|2018-08-01 12:22:15|71272561|7552.95|false|0.00761599|",
        "|2018-08-01 12:22:16|71272562|7552.41|true |0.599052  |").map(parseTransaction)
      val transactions2 = Seq(
        "|2018-08-01 12:22:13|71272555|7552.37|true |0.1       |",
        "|2018-08-01 12:22:13|71272556|7552.39|false|0.40094701|",
        "|2018-08-01 12:22:14|71272557|7552.95|false|0.0036    |",
        "|2018-08-01 12:22:14|71272559|7552.95|false|0.119455  |",
        "|2018-08-01 12:22:15|71272561|7552.95|false|0.00761599|",
        "|2018-08-01 12:22:16|71272562|7552.41|true |0.599052  |",
        "|2018-08-01 12:22:24|71272579|7552.42|false|0.02420833|",
        "|2018-08-01 12:22:25|71272582|7552.41|true |0.020948  |",
        "|2018-08-01 12:22:30|71272594|7550.79|false|0.05      |",
        "|2018-08-01 12:22:32|71272595|7549.92|false|0.4210102 |",
        "|2018-08-01 12:22:33|71272596|7549.92|false|0.06033534|",
        "|2018-08-01 12:22:40|71272602|7549.69|false|0.20200291|",
        "|2018-08-01 12:22:41|71272603|7549.68|true |0.1       |",
        "|2018-08-01 12:22:48|71272614|7549.69|false|0.0024224 |",
        "|2018-08-01 12:22:49|71272615|7549.68|true |0.1       |",
        "|2018-08-01 12:22:49|71272616|7549.68|true |0.42      |",
        "|2018-08-01 12:22:58|71272618|7549.69|false|0.09720503|").map(parseTransaction)

      FakeTimer.clockRealTimeInMillis = Instant.parse("2018-08-01T10:21:32Z").toEpochMilli
      val ((ds1, instant1), (ds2, instant2)) = {
        for {
          (tuple1) <- TransactionDataBatchProducer.processOneBatch(
            IO(transactions1.toDS()),
            Seq.empty[Transaction].toDS(),
            Instant.parse("2018-08-01T10:21:20Z"))

          (tuple2) <- TransactionDataBatchProducer.processOneBatch(
            IO(transactions2.toDS()),
            tuple1._1,
            tuple1._2)
        } yield (tuple1, tuple2)
      }.unsafeRunSync()
      // should fail with 71272554 missing
      // => should have fetched it earlier ?

      val savedTransactions = spark.read.parquet(appConfig.transactionStorePath).as[Transaction].collect()
      savedTransactions.map(_.tid).toSet should === ((transactions1 ++ transactions2).map(_.tid).toSet)


    }


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
