name := "FindIdWikidataWithFewData"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0" ,
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  //"org.apache.spark" %% "spark-streaming" % "2.3.0",
  "org.apache.spark" %% "spark-test-tags" % "1.6.3" % Test,

  "org.apache.pdfbox" % "pdfbox" % "2.0.9",
  "org.scalaj" %% "scalaj-http" % "2.3.0"
)