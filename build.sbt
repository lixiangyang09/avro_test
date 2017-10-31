
lazy val root = (project in file(".")).
  settings(
    name := "SBT_templete",
    version := "1.1-SNAPSHOT"
    // , scalaVersion:= "2.11.11"
  )

libraryDependencies += "org.apache.avro"  %  "avro"  %  "1.8.2"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25"


