package contadamination.bloom

import java.io.File

import com.twitter.algebird.ApproximateBoolean
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest._

/**
 * Created by dahljo on 7/9/15.
 */
class BloomFilterBuilderTest extends FunSuite with Assertions {

  val conf = new SparkConf().setAppName("SparkQ").setMaster("local[1]")
  val sc = new SparkContext(conf)
  val probOfFalsePositive = 0.0005
  val windowSize = 30
  val reference: File = new File("src/test/resources/mt.fasta")

  test("createBloomFilter") {

    val bloomfilterBuilder =
      new BloomFilterBuilder(
        sc,
        probOfFalsePositive,
        windowSize,
        reference)

    val bloomFilter = bloomfilterBuilder.createBloomFilter()

    val first30bases = scala.io.Source.fromFile(reference).getLines().drop(1).next().take(windowSize)
    assert(bloomFilter.contains(first30bases).isTrue)
    assert(!bloomFilter.contains("Z" * windowSize).isTrue)
    //println(bloomFilter)

  }

}
