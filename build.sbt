import sbtprotobuf.{ProtobufPlugin=>PB}

seq(PB.protobufSettings: _*)

name := "google-trace-analysis"

version := "1.0"

scalaVersion := "2.9.1"

scalacOptions := Seq("-deprecation", "-unchecked", "-optimise", "-Yinline")

organization := "edu.berkeley.cs.amplab"

resolvers += "Apache Snapshots" at "https://repository.apache.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-core" % "0.20.205.0",
  "org.apache.hive" % "hive-serde" % "0.7.1-SNAPSHOT",
  "log4j" % "log4j" % "1.2.16",
  "org.spark-project" %% "spark-core" % "0.4-SNAPSHOT",
  "org.spark-project" %% "spark-repl" % "0.4-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "1.6.1" % "test"
)

testOptions in Test += Tests.Argument("-oDF")

protoc in PB.protobufConfig := "./protoc --plugin=protoc-gen-twadoop=./protoc-gen-twadoop --twadoop_out=target/scala-2.9.1/src_managed/main/compiled_protobuf"

fork in run := true

javaOptions++= Seq("-Djava.library.path=" +
    System.getProperty("java.library.path") + ":./native-lib",
    "-Dspark.home=./spark-home",
    "-Dspark.mem=3000m",
    "-Dspark.cache.class=spark.BoundedMemoryCache",
    "-Dspark.diskSpillingCache.cacheDir=/itch/charles-tmp",
    "-Dspark.boundedMemoryCache.memoryFraction=0.10",
    "-Dspark.default.parallelism=800",
    "-Dspark.local.dir=/scratch/charles-tmp",
    "-Dspark.kryo.registrator=edu.berkeley.cs.amplab.MyKryoRegistrator",
    "-Dspark.serializer=spark.KryoSerializer",
    "-Dspark.kryoserializer.buffer.mb=120",
    "-Dcom.sun.management.jmxremote",
    "-Dspark.shuffle.fetcher=spark.ParallelShuffleFetcher",
    "-Dspark.dfs.workDir=/work/charles/spark-dfs",
    "-Xmx3700m", "-Xms2000m") 

mklauncherTask
