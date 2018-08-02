import java.net.URL

import cats.effect.IO
import coinyser.{Transaction, TransactionDataBatchProducer}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

implicit val spark: SparkSession = SparkSession.builder.master("local[*]").appName("coinyser").getOrCreate()
import spark.implicits._
import org.apache.spark.sql.functions._

def filterTxs(txs: Dataset[Transaction]): Dataset[Transaction] = txs.filter("date >= '2018-08-02 07:41:00'")

val ds = filterTxs(spark.read.parquet("/tmp/coinyser/transaction/dt=2018-08-02").as[Transaction])
ds.groupBy(window($"date", "1 minute").as("w")).agg(count($"tid")).sort($"w").show(10000,false)

val ds2 = filterTxs(TransactionDataBatchProducer.readTransactions(IO(Source.fromURL(new URL("https://www.bitstamp.net/api/v2/transactions/btcusd/?time=day")).mkString)).unsafeRunSync())
ds2.groupBy(window($"date", "1 minute").as("w")).agg(count($"tid")).sort($"w").show(10000,false)
