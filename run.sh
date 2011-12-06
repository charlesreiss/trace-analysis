export MASTER='mesos://master@localhost:60060/'
exec ./target/scala-sbt spark.repl.Main

