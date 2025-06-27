name    := "transactional"
version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark"           %% "spark-core"       % "3.5.0",
  "org.apache.spark"           %% "spark-sql"        % "3.5.0",
  "io.delta"                   %% "delta-spark"      % "3.2.0",
  "org.apache.hadoop"           % "hadoop-common"    % "3.4.1",
  "org.apache.hadoop"           % "hadoop-hdfs"      % "3.4.1",
  "com.typesafe.scala-logging" %% "scala-logging"    % "3.9.5",
  "org.scalactic"              %% "scalactic"        % "3.2.19",
  "org.scalatest"              %% "scalatest"        % "3.2.19" % Test,
  "com.github.mrpowers"        %% "spark-fast-tests" % "3.0.1"  % Test
)

Test / fork              := true
Test / parallelExecution := false
