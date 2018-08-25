package coinyser

import java.sql.Timestamp

import cats.effect.IO
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.{Assertion, Matchers, WordSpec}

class BatchProducerIT extends WordSpec with Matchers with SharedSparkSession {

  import testImplicits._

  "BatchProducer.save" should {
    "save a Dataset[Transaction] to parquet" in withTempDir { tmpDir =>
      val transaction1 = Transaction(timestamp = new Timestamp(1532365695000L), tid = 70683282, price = 7740.00, sell = false, amount = 0.10041719)
      val transaction2 = Transaction(timestamp = new Timestamp(1532365693000L), tid = 70683281, price = 7739.99, sell = false, amount = 0.00148564)
      val sourceDS = Seq(transaction1, transaction2).toDS()

      val uri = tmpDir.toURI
      val io: IO[Assertion] =
        for {
          _ <- BatchProducer.save(sourceDS, uri)
          readDS <- IO(spark.read.parquet(uri.toString).as[Transaction])
          assertion <- IO(sourceDS.collect() should contain theSameElementsAs readDS.collect())
        } yield assertion
      io.unsafeRunSync()
    }
  }
}
