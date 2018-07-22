package coinyser

import org.apache.spark.sql.SparkSession

object BatchConsumerApp extends App {
  // In prod, should be a distributed filesystem
  val checkpointDir = "/tmp/coinyser/BatchConsumerApp"
  implicit val kafkaConfig: KafkaConfig = KafkaConfig(
    topic = "ticker_btcusd",
    bootstrapServers = "localhost:9092",
    checkpointLocation = checkpointDir)


  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("BatchConsumer")
    .getOrCreate()
  BatchConsumer.start
}
