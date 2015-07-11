import sbt._

object Version {
  val spark             = "1.4.0"
}

object Library {
  val sparkCore         = "org.apache.spark" %% "spark-core" % Version.spark
  val sparkMLlib        = "org.apache.spark" %% "spark-mllib" % Version.spark

  val adam              = "org.bdgenomics.adam" %% "adam-core" % "0.17.0"

  val cli               = "org.bdgenomics.utils" %% "utils-cli" % "0.2.2"
  val utils             = "org.bdgenomics.utils" %% "utils-misc" % "0.2.2"
  val algebird          = "com.twitter" %% "algebird-core" % "0.10.2"

  val scalaTest         = "org.scalatest" %% "scalatest" % "2.2.4"
}

object Dependencies {

  import Library._

  val contADAMination = Seq(
    sparkCore,
    sparkMLlib,
    adam,
    cli,
    utils,
    algebird,

    scalaTest % "test"
  )
}