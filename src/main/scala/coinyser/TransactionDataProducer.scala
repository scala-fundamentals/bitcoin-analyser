package coinyser

import java.net.URL

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.duration.FiniteDuration
import scala.io.Source

case class AppConfig(topic: String,
                     bootstrapServers: String,
                     checkpointLocation: String,
                     intervalBetweenReads: FiniteDuration,
                     transactionStorePath: String)


object TransactionDataProducer {


  def start(url: URL)(implicit kafkaConfig: AppConfig, spark: SparkSession) = {
    val tickerStream = transactionReadStream(_ => Source.fromURL(url))
    kafkaWriteStream(tickerStream)
    //    consoleWriteStream(tickerStream)
  }

  def consoleWriteStream[A](tickerStream: Dataset[A])
                           (implicit spark: SparkSession): StreamingQuery = {
    tickerStream
      .toJSON
      .writeStream
      .format("console")
      .option("truncate", "false")
      .start()
  }


  def kafkaWriteStream[A](tickerStream: Dataset[A])
                         (implicit kafkaConfig: AppConfig, spark: SparkSession): StreamingQuery = {
    tickerStream
      .toJSON
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
      .option("topic", kafkaConfig.topic)
      .option("checkpointLocation", kafkaConfig.checkpointLocation)
      .start()
  }


  def transactionReadStream(createSource: Long => Source)(implicit spark: SparkSession): Dataset[Transaction] = {
    import spark.implicits._
    val txSchema = Seq.empty[BitstampTransaction].toDS().schema
    val schema = ArrayType(txSchema)
    spark.readStream
      .format("rate")
      .load()
      .filter(pmod($"value", lit(5)) === 0)
      .map(row => createSource(row.getAs[Long]("value")).mkString)
      .select(explode(from_json($"value".cast(StringType), schema)).alias("v"))
      .select(
        $"v.date".cast(LongType).cast(TimestampType).as("date"),
        $"v.tid".cast(IntegerType),
        $"v.price".cast(DoubleType),
        $"v.type".cast(BooleanType).as("sell"),
        $"v.amount".cast(DoubleType))
      .withWatermark("date", "20 second")
      .distinct() // we need to use watermark to use distinct, otherwise it will keep everything in memory
      .as[Transaction]
  }


}


