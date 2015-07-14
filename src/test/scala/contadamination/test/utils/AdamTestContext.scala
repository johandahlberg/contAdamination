package contadamination.test.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext
import org.scalatest.BeforeAndAfter


/**
 * Created by dahljo on 7/13/15.
 */
trait AdamTestContext extends BeforeAndAfter {

  this: ContadaminationSuite =>

  private val master = "local[2]"
  private val appName = "contadamination"

  private var sparkContext: SparkContext = _
  implicit var adamContext: ADAMContext = _

  before {
    if(sparkContext == null) {
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
      System.clearProperty("spark.master.port")

      val conf = new SparkConf()
        .setMaster(master)
        .setAppName(appName)

      sparkContext = new SparkContext(conf)
      adamContext = new ADAMContext(sparkContext)
    }

  }

  after {
    if (sparkContext != null) {
      sparkContext.stop()
      sparkContext = null
      adamContext = null

      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
      System.clearProperty("spark.master.port")
    }
  }

}
