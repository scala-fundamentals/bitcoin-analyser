package coinyser

import java.net.URL

import org.apache.spark.sql.SparkSession

/*
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &


bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ticker_btcusd --from-beginning
 */
object TransactionDataProducerApp extends App {
  // In prod, should be a distributed filesystem
  val checkpointDir = "/tmp/coinyser/TransactionDataProducerApp"
  implicit val kafkaConfig: KafkaConfig = KafkaConfig(
    topic = "ticker_btcusd",
    bootstrapServers = "localhost:9092",
    checkpointLocation = checkpointDir)


  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("coinyser")
    .getOrCreate()

  TransactionDataProducer.start(new URL("https://www.bitstamp.net/api/v2/transactions/btcusd/?time=minute")).awaitTermination()

}
