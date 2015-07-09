import sbt._

object Version {
  val spark        = "1.1.1"
}

object Library {
  val sparkCore      = "org.apache.spark"  % "spark-core_2.10" % Version.spark
  val scalaTest      = "org.scalatest" % "scalatest_2.10" % "2.2.4"
  val adam = "org.bdgenomics.adam" % "adam-core_2.10" % "0.17.0"
  val algebird = "com.twitter" % "algebird-core_2.10" % "0.10.2"
  //val picard = "org.utgenome.thirdparty" % "picard" % "1.86.0"
}

object Dependencies {

  import Library._

  val sparkHadoop = Seq(
    sparkCore,
    adam,
    scalaTest,
    algebird
  )
}