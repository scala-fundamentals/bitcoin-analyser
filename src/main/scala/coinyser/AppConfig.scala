package coinyser

import scala.concurrent.duration.FiniteDuration

case class AppConfig(topic: String,
                     bootstrapServers: String,
                     checkpointLocation: String,
                     firstInterval: FiniteDuration,
                     intervalBetweenReads: FiniteDuration,
                     transactionStorePath: String)
