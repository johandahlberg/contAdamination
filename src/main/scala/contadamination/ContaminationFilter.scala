package contadamination

import com.twitter.algebird.BloomFilter
import org.apache.spark.mllib.rdd.RDDFunctions
import org.bdgenomics.adam.rdd.ADAMContext

/**
 * Created by chris-zen on 11/07/2015.
 */
class ContaminationFilter(val winSize: Int = 32, val fpr: Double = 0.1) {

  def withWindowSize(winSize: Int): ContaminationFilter = {
    new ContaminationFilter(winSize, fpr)
  }

  def withFalsePositiveRate(fpr: Double): ContaminationFilter = {
    new ContaminationFilter(winSize, fpr)
  }

  def build(path: String)(implicit adamContext: ADAMContext): ContaminationModel = {
    val fragments = adamContext.loadFasta(path, winSize).cache()
    require(fragments.count() > 0, "At least one fragment is required")

    // TODO check that there is only one contigName per file (right now we don't support multiple contigs per file)
    val contigName = fragments.first().getContig.getContigName
    
    val slidingFragments = new RDDFunctions(fragments).sliding(2).flatMap({
      case Array(frag) =>
        Array(frag.getFragmentSequence)

      case Array(frag1, frag2) =>
        val seq1 = frag1.getFragmentSequence
        val seq2 = frag2.getFragmentSequence
        val seq = seq1 + seq2
        // TODO make sure that seq2 size equals seq1 size (if not fill seq2 with dots or something up to the size of seq1)
        for (i <- 0 until frag1.getFragmentSequence.size) yield seq.substring(i, winSize)
    }).cache()

    // TODO I have concerns about the high number of entries and the user asking for a low FPR
    val numEntries = slidingFragments.count().toInt
    // TODO Broadcasting of the bloom filter monoid would be more efficient,
    // the alternative to avoid serialization for every partition is to create it inside the map
    // (not sure about the cost of its creation)
    val bfm = BloomFilter(numEntries, fpr)
    val bf = slidingFragments.map(bfm.create(_)).reduce((x, y) => x ++ y)
    val m = Map(contigName -> bf)
    new ContaminationModel(winSize, m)
  }
}
