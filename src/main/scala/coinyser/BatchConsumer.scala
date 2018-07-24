package coinyser

import java.sql.Timestamp

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.streaming.StreamingContext






object BatchConsumer {
  def start(implicit kafkaConfig: KafkaConfig, spark: SparkSession) = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val schema = Seq.empty[Ticker].toDS().schema
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

    val df = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
      .option("subscribe", kafkaConfig.topic)
      .load()
      .select(from_json(col("value").cast("string"), schema).alias("v"))
      .select("v.*").as[Ticker]
    df.write.parquet("/tmp/coinyser/BatchConsumer/")
  }

}


