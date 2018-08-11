package coinyser

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

object StreamingConsumer {
  def fromJson(df: DataFrame): Dataset[Transaction] = {
    import df.sparkSession.implicits._
    val schema = Seq.empty[Transaction].toDS().schema
    df.select(from_json(col("value").cast("string"), schema).alias("v"))
      .select("v.*").as[Transaction]
  }

}
