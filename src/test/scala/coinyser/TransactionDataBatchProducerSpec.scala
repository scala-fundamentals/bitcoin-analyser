package coinyser

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp
import java.time.OffsetDateTime

import coinyser.TransactionDataProducerSpec._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.duration._
import scala.io.Source

class TransactionDataBatchProducerSpec extends WordSpec with Matchers with BeforeAndAfterAll with TypeCheckedTripleEquals with Eventually {

  override implicit def patienceConfig: PatienceConfig = new PatienceConfig(10.seconds, 100.millis)

  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("coinyser")
    .getOrCreate()

  val checkpointDir: File = Files.createTempDirectory("TransactionDataProducerSpec_checkpoint").toFile
  val transactionStoreDir: File = Files.createTempDirectory("TransactionDataBatchProducerSpec_transactions").toFile

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(checkpointDir)
    FileUtils.deleteDirectory(transactionStoreDir)
  }

  implicit val kafkaConfig: AppConfig = AppConfig(
    topic = "transaction_btcusd",
    bootstrapServers = "localhost:9092",
    checkpointLocation = checkpointDir.toString,
    transactionStorePath = transactionStoreDir.toString
  )

  import spark.implicits._


  val transaction1 = Transaction(date = new Timestamp(1532365695000L), tid = 70683282, price = 7740.00, sell = false, amount = 0.10041719)
  val transaction2 = Transaction(date = new Timestamp(1532365693000L), tid = 70683281, price = 7739.99, sell = false, amount = 0.00148564)

  "TransactionDataBatchProducer.readTransactions" should {
    "create a Dataset[Transaction] from a constant Source" in {
      val source = Source.fromString(
        """[{"date": "1532365695", "tid": "70683282", "price": "7740.00", "type": "0", "amount": "0.10041719"},
          |{"date": "1532365693", "tid": "70683281", "price": "7739.99", "type": "0", "amount": "0.00148564"}]""".stripMargin)

      val ds: Dataset[Transaction] = TransactionDataBatchProducer.readTransactions(source)
      val data = ds.collect()
      data should contain theSameElementsAs Seq(transaction1, transaction2)
    }

    "fail if the json payload is incorrect" in pending
  }

  "TransactionDataBatchProducer.readSaveRepeatedly" should {
    "fetch new transactions every 10s and save them" in {
      def source = Source.fromString {
        val now = OffsetDateTime.now().toEpochSecond
        val transactions = Seq.tabulate(10)(i =>
          s"""{"date": "${now - i}", "tid": "${now - i}", "price": "7740.00", "type": "0", "amount": "0.10041719"}"""
        )
        "[" + transactions.mkString(",") + "]"
      }
      TransactionDataBatchProducer.readSaveRepeatedly(10, source)

    }
  }


}



