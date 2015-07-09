package contadamination.bloom

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

/**
 * Created by dahljo on 7/9/15.
 */
class BloomFilterBuilderTest extends FunSuite {

  val conf = new SparkConf().setAppName("SparkQ").setMaster("local[1]")
  val sc = new SparkContext(conf)
  val windowSize = 30
  val reference: File = ???
  val numHashes = ???
  val width = ???
  val seed = 2000

  test("createBloomFilter") {

    val bloomfilterBuilder =
      new BloomFilterBuilder(sc,
        windowSize,
        reference,
        numHashes,
        width,
        seed)

  }

}
