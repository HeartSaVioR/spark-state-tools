lazy val scala212 = "2.12.10"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

scalaVersion := scala212
crossScalaVersions := supportedScalaVersions

name := "spark-state-tools"

organization := "net.heartsavior.spark"

description := "State tools for Apache Spark which lets you play with states " +
  "in your structured streaming queries."

version := "0.3.0-SNAPSHOT"

licenses += ("Apache 2.0 License", url("http://www.apache.org/licenses/LICENSE-2.0.html"))

cancelable := true

developers := List(
  Developer(
    "HeartSaVioR",
    "Jungtaek Lim",
    "kabhwan@gmail.com",
    url("https://github.com/HeartSaVioR/")
  )
)

javacOptions ++= Seq(
  "-Xlint:deprecation",
  "-Xlint:unchecked",
  "-source", "1.8",
  "-target", "1.8",
  "-g:vars"
)

val sparkVersion = "2.4.0"
val scalatestVersion = "3.0.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.scalatest" %% "scalatest" % "3.0.3" % Test withSources(),
  "junit" % "junit" % "4.12" % Test
)


Global / onChangedBuildSource := ReloadOnSourceChanges


logBuffered in Test := false
parallelExecution in Test := false

logLevel := Level.Warn

// Only show warnings and errors on the screen for compilations.
// This applies to both test:compile and compile and is Info by default
logLevel in compile := Level.Warn

// Level.INFO is needed to see detailed output when running tests
logLevel in test := Level.Info

resolvers ++= Seq(
)

scalacOptions ++= Seq(
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-encoding", "utf-8", // Specify character encoding used by source files.
  "-explaintypes", // Explain type errors in more detail.
  "-feature", // Emit warning and location for usages of features that should be
              // imported explicitly.
  "-language:existentials", // Existential types (besides wildcard types) can be written
                            // and inferred
  "-language:experimental.macros", // Allow macro definition (besides implementation and
                                   // application)
  "-language:higherKinds", // Allow higher-kinded types
  "-language:implicitConversions", // Allow definition of implicit functions called views
  "-unchecked", // Enable additional warnings where generated code depends on assumptions.
  "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
  "-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or
                       // creating a tuple) to match the receiver.
  "-Ypartial-unification", // Enable partial unification in type constructor inference
  "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
  "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Ywarn-nullary-unit", // Warn when nullary methods return Unit.
)

scalacOptions ++=
  scalaVersion {
    case sv if sv.startsWith("2.12") => List(
      "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
    )

    case _ => Nil
  }.value

// The REPL can’t cope with -Ywarn-unused:imports or -Xfatal-warnings
// so turn them off for the console
scalacOptions in(Compile, console) --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings")

val gitHubId = "HeartSaVioR"

scalacOptions in(Compile, doc) ++= baseDirectory.map {
  bd: File =>
    Seq[String](
      "-sourcepath", bd.getAbsolutePath,
      "-doc-source-url", s"https://github.com/$gitHubId/$name/tree/master€{FILE_PATH}.scala"
    )
}.value

scmInfo := Some(
  ScmInfo(
    url(s"https://github.com/$gitHubId/$name"),
    s"git@github.com:$gitHubId/$name.git"
  )
)

watchTriggeredMessage in ThisBuild := Watch.clearScreenOnTrigger

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

scalastyleConfig := file("scalastyle-config.xml")