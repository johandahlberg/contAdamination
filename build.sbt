name := "contAdamination"

scalaVersion := Version.scala

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/"

libraryDependencies ++= Dependencies.contADAMination

releaseSettings

scalariformSettings

checksums := Nil
