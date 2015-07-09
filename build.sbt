
name := """contAdamination"""

scalaVersion := "2.10.4"

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/"

libraryDependencies ++= Dependencies.sparkHadoop

releaseSettings

scalariformSettings

checksums := Nil
