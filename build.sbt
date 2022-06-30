name := "zhttp_cqrs"

organization := "ag.dc"

version := "0.2.1"

scalaVersion := "3.1.2"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio"         % "2.0.0-RC6",
  "io.d11"  %% "zhttp"       % "2.0.0-RC9",
  "io.d11"  %% "zhttp-test"  % "2.0.0-RC9",
  "io.d11" %% "zhttp-test"   % "2.0.0-RC9" % Test,
  "dev.zio" %% "zio-json"    % "0.3.0-RC8",
)

ThisBuild / versionScheme := Some("early-semver")

githubOwner := "dc-ag"
githubRepository := "zhttp-cqrs"
