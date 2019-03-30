
name := "spark-state-tools"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += Resolver.mavenLocal
resolvers ++= List("Local Maven Repository" at "file:///" + Path.userHome.absolutePath + "/.m2/repository")

// scallop is MIT licensed
libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"

val sparkVersion = "2.4.0"
val scopeForSparkArtifacts = "compile"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion % scopeForSparkArtifacts

libraryDependencies += "org.scalacheck" % "scalacheck_2.11" % "1.13.5" % "test"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.3" % "test"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion % "test" classifier "tests"
libraryDependencies += "org.apache.spark" % "spark-catalyst_2.11" % sparkVersion % "test" classifier "tests"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "test" classifier "tests"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  // start -- Spark 3.0.0-SNAPSHOT...
  case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  // end -- Spark 3.0.0-SNAPSHOT...
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  // below is due to Apache Arrow...
  case "git.properties" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

target in assembly := file("build")

/* including scala bloats your assembly jar unnecessarily, and may interfere with
   spark runtime */
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}.jar"
