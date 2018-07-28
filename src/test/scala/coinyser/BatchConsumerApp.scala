package coinyser

import org.apache.spark.sql.SparkSession

object BatchConsumerApp extends App {
  // In prod, should be a distributed filesystem
  val checkpointDir = "/tmp/coinyser/BatchConsumerApp"
  implicit val kafkaConfig: AppConfig = AppConfig(
    topic = "transaction_btcusd",
    bootstrapServers = "localhost:9092",
    checkpointLocation = checkpointDir,
    transactionStorePath = "???"
  )


  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("BatchConsumer")
    .getOrCreate()
  BatchConsumer.fromKafka
}
