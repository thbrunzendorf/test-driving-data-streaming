name := "scio-sample"

version := "0.1"

scalaVersion := "2.12.4"

val scioVersion = "0.5.2"
val beamVersion = "2.4.0"

libraryDependencies ++= Seq(
  "com.spotify"              %% "scio-core"                % scioVersion,
  "org.apache.beam"          %  "beam-runners-direct-java" % beamVersion,
  "org.slf4j"                %  "slf4j-simple"             % "1.7.25",
  "org.apache.logging.log4j" %  "log4j-core"               % "2.9.1",
  "com.spotify"              %% "scio-test"                % scioVersion % "test",
  "org.scalatest"            %% "scalatest"                % "3.0.4"     % "test"
)
