package coinyser.draft

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingProducerApp extends App {

  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("StreamingProducer")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
  val stream = ssc.receiverStream(new BitstampReceiver())
  stream.print()
  ssc.start()
  ssc.awaitTermination()
}
