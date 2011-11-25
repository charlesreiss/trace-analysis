resolvers += Resolver.file("included-plugins",
file("/home/eecs/charles/trace-analysis/plugin-lib"))(
  Patterns("[artifact]-[revision].[ext]"))

addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.2")
