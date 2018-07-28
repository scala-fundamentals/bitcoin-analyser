package coinyser

import java.net.URL
import java.sql.Timestamp

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.streaming.StreamingContext






object BatchConsumer {
  def fromEndpoint(url: URL)(implicit spark: SparkSession) = {
    spark.read.json()

  }

  def fromKafka(implicit kafkaConfig: AppConfig, spark: SparkSession): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val schema = Seq.empty[Transaction].toDS().schema
/*
    val inStream: Dataset[Ticker] = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
      .option("subscribe", kafkaConfig.topic)
      .load()
      .select(from_json(col("value").cast("string"), schema).alias("v"))
      .select("v.*").as[Ticker]

    inStream.writeStream.format("console").start()
    //        spark.read.json(ds)
    */

    // save 24 hours of tx ?? Could be a more gentle intro to spark, without streaming and without kafka
    // also, easier to run it once a day to get some historical data
    val df = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
      .option("subscribe", kafkaConfig.topic)
      .load()
      .select(from_json(col("value").cast("string"), schema).alias("v"))
      .select("v.*").as[Transaction]
    df.write.parquet("/tmp/coinyser/BatchConsumer/")
  }

}


