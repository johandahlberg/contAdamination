import sbt._

object Version {
  val spark        = "1.4.0"
}

object Library {
  val sparkCore      = "org.apache.spark"  % "spark-core_2.11" % Version.spark
  val scalaTest      = "org.scalatest" % "scalatest_2.11" % "2.2.4"
  val adam = "org.bdgenomics.adam" % "adam-core_2.11" % "0.17.0"
  val cli = "org.bdgenomics.utils" % "utils-cli_2.11" % "0.2.2"
  val utils = "org.bdgenomics.utils" % "utils-misc_2.11" % "0.2.2"
  val algebird = "com.twitter" % "algebird-core_2.11" % "0.10.2"
}

object Dependencies {

  import Library._

  val sparkHadoop = Seq(
    sparkCore,
    adam,
    cli,
    utils,
    scalaTest,
    algebird
  )
}