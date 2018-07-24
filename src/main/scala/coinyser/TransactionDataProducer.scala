package coinyser

import java.io.File
import java.net.URL
import java.util.Properties

import scala.collection.JavaConversions._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._

import scala.io.Source

case class KafkaConfig(topic: String,
                       bootstrapServers: String,
                       checkpointLocation: String)

object TransactionDataProducer {


  def start(url: URL)(implicit kafkaConfig: KafkaConfig, spark: SparkSession) = {
    val tickerStream = transactionReadStream(_ => Source.fromURL(url))
    kafkaWriteStream(tickerStream)
  }

  def kafkaWriteStream[A](tickerStream: Dataset[A])
                         (implicit kafkaConfig: KafkaConfig, spark: SparkSession): StreamingQuery = {
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
    spark.readStream.format("rate")
      .load()
      .map(row => createSource(row.getAs[Long]("value")).mkString)
      .select(explode(from_json($"value".cast(StringType), schema)).alias("v"))
      .select(
        $"v.date".cast(LongType).cast(TimestampType).as("date"),
        $"v.tid".cast(IntegerType),
        $"v.price".cast(DoubleType),
        $"v.type".cast(BooleanType).as("sell"),
        $"v.amount".cast(DoubleType))
      .withWatermark("date", "1 second")
      .distinct() // we need to use watermark to use distinct, otherwise it will keep everything in memory
      .as[Transaction]
  }


}


