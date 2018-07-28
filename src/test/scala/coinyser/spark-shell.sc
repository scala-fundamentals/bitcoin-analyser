import org.apache.spark.sql.SparkSession

implicit val spark: SparkSession = SparkSession.builder.master("local[*]").appName("coinyser").getOrCreate()
import spark.implicits._
import org.apache.spark.sql.functions._

val ds = spark.read.parquet("/tmp/coinyser/transaction/2018-07-26")
ds.groupBy(window($"date", "1 minute").as("w")).agg(count($"tid")).sort($"w").show(10000,false)