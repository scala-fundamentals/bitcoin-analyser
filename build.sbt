name := "bitcoin-analyser"

version := "0.1"

scalaVersion := "2.11.11"
val sparkVersion = "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "test-sources"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "test-sources"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "test-sources"
libraryDependencies += "org.lz4" % "lz4-java" % "1.4.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
libraryDependencies += "org.typelevel" %% "cats-core" % "1.1.0"
libraryDependencies += "org.typelevel" %% "cats-effect" % "1.0.0-RC2"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
libraryDependencies += "com.pusher" % "pusher-java-client" % "1.8.0"

scalacOptions += "-Ypartial-unification"

// Avoids SI-3623
target := file("/tmp/sbt/bitcoin-analyser")
