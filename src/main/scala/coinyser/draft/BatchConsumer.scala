package coinyser.draft

import java.net.URL

import coinyser.{AppConfig, Transaction}
import org.apache.spark.sql.SparkSession






object BatchConsumer {
  def fromEndpoint(url: URL)(implicit spark: SparkSession) = {
    spark.read.json()

  }

  def fromKafka(implicit kafkaConfig: AppConfig, spark: SparkSession): Unit = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
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


