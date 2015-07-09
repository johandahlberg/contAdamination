package contadamination.bloom

import java.io.File

import com.twitter.algebird.BloomFilterMonoid
import com.twitter.algebird.BF
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.NucleotideContigFragment

/**
 * Created by dahljo on 7/9/15.
 */
class BloomFilterBuilder(sparkContext: SparkContext,
                         probabablityOfFalsePositive: Double,
                         windowSize: Int,
                         reference: File) {

  case class BloomFilterConfig(numHashes: Int, width: Int, seed: Int)

  private def createBloomFilterConfig(probabablityOfFalsePositive: Double): BloomFilterConfig = {
    // TODO Make better reasonable deaults.
    //    val width = 10000000
    //    val numOfHashes = ((probabablityOfFalsePositive.toDouble / width.toDouble) * math.log(2)).toInt
    //    println(s"numOfHashes: $numOfHashes")
    BloomFilterConfig(numHashes = 6, width = 1000000, seed = 1)
  }

  private def referenceToWindow(reference: File, windowSize: Int): RDD[String] = {
    // TODO Check the fragment lenght option. /JD 20150709
    val fasta = sparkContext.loadFasta(reference.getPath(), 10000L)
    val sliding = (x: String) => x.sliding(windowSize)
    val windows: RDD[String] = fasta.map(x => x.getFragmentSequence).flatMap(sliding)
    windows
  }

  def createBloomFilter(): BF = {
    val bloomFilterConfig = createBloomFilterConfig(probabablityOfFalsePositive)
    val bloomFilterMonind =
      BloomFilterMonoid(bloomFilterConfig.numHashes, bloomFilterConfig.width, bloomFilterConfig.seed)

    val bloomFilters =
      for {
        window <- referenceToWindow(reference, windowSize)
      } yield bloomFilterMonind.create(window)

    val finalBloomFilter =
      bloomFilters.reduce((x, y) => bloomFilterMonind.plus(x, y))

    finalBloomFilter
  }
}
