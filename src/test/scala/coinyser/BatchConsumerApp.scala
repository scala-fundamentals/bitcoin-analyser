package coinyser

import org.apache.spark.sql.SparkSession

object BatchConsumerApp extends App {
  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("coinyser")
    .getOrCreate()
  BatchConsumer.start.awaitTermination()
}
