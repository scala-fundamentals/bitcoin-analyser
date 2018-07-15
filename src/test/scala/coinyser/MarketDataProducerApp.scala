package coinyser

import java.net.URL

import org.apache.spark.sql.SparkSession

/*
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &


bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ticker_btcusd --from-beginning
 */
object MarketDataProducerApp extends App {
//  MarketDataProducer.sendFile(getClass.getClassLoader.getResource("ticker_btcusd_20180708_1531056455.json"))
//  MarketDataProducer.sendFile(getClass.getClassLoader.getResource("ticker_btcusd_20180708_1531056459.json"))

  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("coinyser")
    .getOrCreate()
//  MarketDataProducer.start(getClass.getClassLoader.getResource("ticker_btcusd_20180708_1531056459.json")).awaitTermination()
  MarketDataProducer.start(new URL("https://www.bitstamp.net/api/v2/ticker/btcusd")).awaitTermination()

}
