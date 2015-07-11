
import com.typesafe.sbt.SbtNativePackager._

name := """contAdamination"""

scalaVersion := "2.11.7"

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/"

libraryDependencies ++= Dependencies.contADAMination

releaseSettings

scalariformSettings

// TODO Fix this once the Adam dependency has 
//       correct checksums. /JD 20150711
checksums := Nil

// Packaging
packageArchetype.java_application

maintainer := "Johan Dahlberg <johan.dahlberg@medsci.uu.se>"

packageSummary := "Distributed contamination detection for short read sequencing"

packageDescription := """contAdamination uses bloom filters to look for contamination in short read sequencing data."""

