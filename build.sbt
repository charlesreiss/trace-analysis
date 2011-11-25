import sbtprotobuf.{ProtobufPlugin=>PB}

seq(PB.protobufSettings: _*)

name := "google-trace-analysis"

version := "1.0"

scalaVersion := "2.9.1"

organization := "edu.berkeley.cs.amplab"

resolvers += "Apache Snapshots" at "https://repository.apache.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-core" % "0.20.205.0",
  "org.apache.hive" % "hive-serde" % "0.7.1-SNAPSHOT",
  "log4j" % "log4j" % "1.2.16"
)

protoc in PB.protobufConfig := "protoc --plugin=protoc-gen-twadoop=./protoc-gen-twadoop --twadoop_out=target/scala-2.9.1/src_managed/main/compiled_protobuf"
