import java.net.URL

import cats.effect.IO
import coinyser.{Transaction, BatchProducer}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

implicit val spark: SparkSession = SparkSession.builder.master("local[*]").appName("coinyser").getOrCreate()
import spark.implicits._
import org.apache.spark.sql.functions._

def filterTxs(txs: Dataset[Transaction]): Dataset[Transaction] = txs.filter("date >= '2018-08-03 09:06:00'")

val ds2 = filterTxs(BatchProducer.readTransactions(IO(Source.fromURL(new URL("https://www.bitstamp.net/api/v2/transactions/btcusd/?time=day")).mkString)).unsafeRunSync())
val grp2 = ds2.groupBy(window($"date", "1 minute").as("w2")).agg(count($"tid").as("cnt2"))

val ds = filterTxs(spark.read.parquet("/tmp/coinyser/transaction/dt=2018-08-03").as[Transaction])
val grp1 = ds.groupBy(window($"date", "1 minute").as("w1")).agg(count($"tid").as("cnt1"))
grp1.join(grp2, $"w1" === $"w2", "full_outer").sort($"w1").filter("cnt1 != cnt2").show(1000, false)

