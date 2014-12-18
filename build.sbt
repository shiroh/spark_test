name := "citi-test"
 
version := "1.0"
 
scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.1.0"
libraryDependencies += "log4j" % "log4j" % "1.2.14"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"
libraryDependencies += "com.twitter" %% "util-collection" % "6.12.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.1.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.4.0"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList("META-INF", "mai	qlcap")               => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

