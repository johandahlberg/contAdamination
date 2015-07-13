package contadamination.test.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext
import org.scalatest.{BeforeAndAfterAll, Suite, BeforeAndAfter}

object AdamTestContextSingleton {

  def createSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("contadamination").setMaster("local[1]")
    val sc = new SparkContext(conf)
    sc
  }

  val sparkContext = createSparkContext()
  val adamContext = new ADAMContext(sparkContext)
}

/**
 * Created by dahljo on 7/13/15.
 */
trait AdamTestContext {

  this: Suite =>

  implicit val adamContext = AdamTestContextSingleton.adamContext

}
