
name := """contAdamination"""

scalaVersion := "2.11.1"

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/"

libraryDependencies ++= Dependencies.sparkHadoop

releaseSettings

scalariformSettings

checksums := Nil
