package contadamination.bloom

import java.io.File

import com.twitter.algebird.ApproximateBoolean
import contadamination.test.utils.{AdamTestContext, ContadaminationSuite}
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest._

/**
 * Created by dahljo on 7/9/15.
 */
class BloomFilterBuilderTest extends ContadaminationSuite with AdamTestContext {

  val probOfFalsePositive = 0.0005
  val windowSize = 30
  val reference: File = new File("src/test/resources/mt.fasta")

  behavior of "BloomFilterBuilder"

  it should "construct a correct filer" in {
    val bloomfilterBuilder =
      new BloomFilterBuilder(
        adamContext,
        probOfFalsePositive,
        windowSize)

    val bloomFilter = bloomfilterBuilder.createBloomFilter(reference)

    val first30bases = scala.io.Source.fromFile(reference).getLines().drop(1).next().take(windowSize)
    assert(bloomFilter.contains(first30bases).isTrue)
    assert(!bloomFilter.contains("Z" * windowSize).isTrue)
  }


}
