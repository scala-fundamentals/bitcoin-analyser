package coinyser

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp

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

class TransactionDataProducerSpec extends WordSpec with Matchers with BeforeAndAfterAll with TypeCheckedTripleEquals with Eventually {

  override implicit def patienceConfig: PatienceConfig = new PatienceConfig(10.seconds, 100.millis)

  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("coinyser")
    .getOrCreate()

  val checkpointDir: File = Files.createTempDirectory("TransactionDataProducerSpec_checkpoint").toFile

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(checkpointDir)
  }

  implicit val kafkaConfig: KafkaConfig = KafkaConfig(
    topic = "transaction_btcusd",
    bootstrapServers = "localhost:9092",
    checkpointLocation = checkpointDir.toString)

  import spark.implicits._


  val transaction1 = Transaction(date = new Timestamp(1532365695000L), tid = 70683282, price = 7740.00, sell = false, amount = 0.10041719)
  val transaction2 = Transaction(date = new Timestamp(1532365693000L), tid = 70683281, price = 7739.99, sell = false, amount = 0.00148564)

  "TransactionDataProducer.tickerReadStream" should {
    "create a stream of Transaction data from a constant Source with unique values" in {
      val stream = TransactionDataProducer.transactionReadStream(createConstantSource)
      val data = processReadStream(stream, 4)
      data should contain theSameElementsAs Seq(transaction1, transaction2)
    }

    "fail if the json payload is incorrect" in pending
  }

  "TransactionDataProducer.kafkaWriteStream" should {
    "write a stream of Transactions to Kafka" in {
      implicit val sqlc: SQLContext = spark.sqlContext
      val tickerStream = MemoryStream[Transaction]
      tickerStream.addData(transaction1, transaction2)

      val kafkaStream = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
        .option("subscribe", kafkaConfig.topic)
        .load()
        .select($"value")
        .as[String]

      val streamingQuery = TransactionDataProducer.kafkaWriteStream(tickerStream.toDS())
      // Order is not guaranteed: each mini-batch is processed in parallel
      processReadStream(kafkaStream, 2) should contain theSameElementsAs Seq(
        """{"date":"2018-07-23T19:08:15.000+02:00","tid":70683282,"price":7740.0,"sell":false,"amount":0.10041719}""",
        """{"date":"2018-07-23T19:08:13.000+02:00","tid":70683281,"price":7739.99,"sell":false,"amount":0.00148564}""")

      streamingQuery.stop()
    }

  }

  def processReadStream[A: Encoder](stream: Dataset[A], expectedTotalInputRows: Int)(implicit spark: SparkSession): Seq[A] = {
    val query = stream
      .writeStream
      .format("memory")
      .queryName("Output")
      .outputMode(OutputMode.Append())
      .start()

    eventually {
      val totalRows = query.recentProgress.map(_.numInputRows).sum
      //println(query.recentProgress.map(p => (p.batchId, p.numInputRows)).mkString(", "))
      totalRows.toInt should be >= expectedTotalInputRows
    }
    query.stop()
    spark.sql("select * from Output").as[A].collect().toSeq
  }

}

object TransactionDataProducerSpec {
  // In companion object to avoid TaskNotSerializable

  def createConstantSource(msgCount: Long): Source =
    Source.fromString(
      """[{"date": "1532365695", "tid": "70683282", "price": "7740.00", "type": "0", "amount": "0.10041719"},
        |{"date": "1532365693", "tid": "70683281", "price": "7739.99", "type": "0", "amount": "0.00148564"}]""".stripMargin)


}

