
name := "contAdamination"

scalaVersion := "2.11.7"

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/"

libraryDependencies ++= Dependencies.contADAMination

releaseSettings

scalariformSettings

checksums := Nil
