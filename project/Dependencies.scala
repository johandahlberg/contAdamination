import sbt._

object Version {
  val scala             = "2.11.7"
  val spark             = "1.4.0"
  val bdgAdam           = "0.17.0"
  val bdgUtils          = "0.2.2"
}

object Library {
  val sparkCore         = "org.apache.spark" %% "spark-core" % Version.spark
  val sparkMLlib        = "org.apache.spark" %% "spark-mllib" % Version.spark

  val bdgAdam           = "org.bdgenomics.adam" %% "adam-core" % Version.bdgAdam
  val bdgCli            = "org.bdgenomics.utils" %% "utils-cli" % Version.bdgUtils
  val bdgUtils          = "org.bdgenomics.utils" %% "utils-misc" % Version.bdgUtils
  
  val algebird          = "com.twitter" %% "algebird-core" % "0.10.2"

  val scalaTest         = "org.scalatest" %% "scalatest" % "2.2.4"
}

object Dependencies {

  import Library._

  val contADAMination = Seq(
    sparkCore,
    sparkMLlib,
    bdgAdam,
    bdgCli,
    bdgUtils,
    algebird,

    scalaTest % "test",
    bdgUtils % "test"
  )
}