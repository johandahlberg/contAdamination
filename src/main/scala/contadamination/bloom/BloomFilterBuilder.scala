package contadamination.bloom

import java.io.File

import com.twitter.algebird.{BloomFilter, BloomFilterMonoid, BF}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.NucleotideContigFragment

/**
 * Created by dahljo on 7/9/15.
 */
class BloomFilterBuilder(sparkContext: ADAMContext, probabilityOfFalsePositive: Double, windowSize: Int) {

  private def referenceToWindow(reference: File, windowSize: Int): RDD[String] = {
    // TODO Check the fragment lenght option. /JD 20150709
    val fasta = sparkContext.loadFasta(reference.getPath(), 10000L)
    val sliding = (x: String) => x.sliding(windowSize)
    val windows: RDD[String] = fasta.map(x => x.getFragmentSequence).flatMap(sliding)
    windows
  }

  def createBloomFilter(reference: File): BF = {

    // TODO This should not be hard-coded. Make some reasonable
    // estimate based on k-mer complexity of genome perhaps?
    // JD 20150710
    val numberOfEntries = 100000
    val bloomFilterMonoid =
      BloomFilter(numberOfEntries, probabilityOfFalsePositive)

    val bloomFilters =
      for {
        window <- referenceToWindow(reference, windowSize)
      } yield bloomFilterMonoid.create(window)

    val finalBloomFilter =
      bloomFilters.reduce((x, y) => bloomFilterMonoid.plus(x, y))

    finalBloomFilter
  }
}
