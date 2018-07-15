name := "bitcoin-analyser"

version := "0.1"

scalaVersion := "2.11.11"

// See https://github.com/sbt/sbt/issues/3618
val workaroundForPackagingType = {
  sys.props += "packaging.type" -> "jar"
}

//libraryDependencies += "javax.ws.rs" % "javax.ws.rs-api" % "2.1"
//libraryDependencies += "org.knowm.xchange" % "xchange-core" % "4.3.7"
//libraryDependencies += "org.knowm.xchange" % "xchange-bitstamp" % "4.3.7"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
libraryDependencies += "org.typelevel" %% "cats-core" % "1.1.0"
libraryDependencies += "org.typelevel" %% "cats-laws" % "1.1.0"
scalacOptions += "-Ypartial-unification"
