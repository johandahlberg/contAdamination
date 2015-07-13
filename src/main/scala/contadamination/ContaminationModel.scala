package contadamination

import com.twitter.algebird.{ BloomFilterMonoid, BloomFilter, BF }
import contadamination.ContaminationModel.{ ContaminationStats, FilterContext }
import org.apache.spark.mllib.rdd.RDDFunctions
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.AlignmentRecord

object ContaminationModel {

  val defaultWinSize = 32
  val defaultFpr = 0.1
  val defaultSeed = 0

  case class FilterContext(
    name: String, // Contig name
    path: String, // Reference path
    numEntries: Int, // Number of sliding windows
    fpr: Double, // False Positive Rate
    bloomFilter: BF // Bloom filter
    )

  case class ContaminationStats(
    numReads: Int, // Total number of reads checked
    numNegatives: Int, // True Negatives (number of negative reads)
    rate: Double // Contamination rate as the number of negative reads over the total number of reads
    )

  /**
   * Build a ContaminationModel from a sequence of paths
   * @param paths Sequence of FASTA files for the reference contigs
   * @param winSize Sliding window size
   * @param fpr False Positive Rate
   * @param adamContext ADAM Context
   * @return a ContaminationModel
   */
  def apply(paths: Seq[String], winSize: Int = defaultWinSize, fpr: Double = defaultFpr)(implicit adamContext: ADAMContext): ContaminationModel = {
    paths.map(ContaminationModel(_, winSize, fpr)).reduce((x, y) => x ++ y)
  }

  /**
   * Build a ContaminationModel from a path
   * @param path FASTA file with the reference contig
   * @param winSize Sliding window size
   * @param fpr False Positive Rate for the BloomFilter
   * @param adamContext ADAM Context
   * @return a ContaminationModel
   */
  def apply(path: String, winSize: Int, fpr: Double)(implicit adamContext: ADAMContext): ContaminationModel = {
    val fragments = adamContext.loadFasta(path, winSize).cache()
    require(fragments.count() > 0, "At least one fragment is required")

    // TODO check that there is only one contigName per file (right now we assume there is only one contig per file)
    val contigName = fragments.first().getContig.getContigName


    // TODO We should not be padding the string, but rather skip once we get partial hits. /JD 20150713
    val slidingFragments = new RDDFunctions(fragments).sliding(2).flatMap({
      case Array(frag) =>
        val seq = frag.getFragmentSequence
        for (i <- 0 until seq.size) yield seq.substring(i, winSize - i) + List.fill(i)(".").mkString

      case Array(frag1, frag2) =>
        val seq1 = frag1.getFragmentSequence
        val seq2 = frag2.getFragmentSequence
        val seq = seq1 + (if (seq2.size == seq1.size) seq2 else seq2 + List.fill(math.abs(seq1.size - seq2.size))(".").mkString)
        for (i <- 0 until seq1.size) yield seq.substring(i, winSize)
    }).cache()

    // TODO I have concerns about the high number of entries and the user asking for a low FPR
    // TODO We actually need to count the unique elements, not all elements!
    val numEntries = slidingFragments.count().toInt

    // TODO check that the cost of creating a new bloomfilter per fragment is not too high
    val width = BloomFilter.optimalWidth(numEntries, fpr)
    val numHashes = BloomFilter.optimalNumHashes(numEntries, width)
    slidingFragments
      .map(frag => ContaminationModel.internalBuild(numEntries, fpr, numHashes, width, winSize, path, contigName, frag))
      .reduce((x, y) => x ++ y)
  }

  /**
   * Internal builder for a ContaminationModel from a single fragment.
   * @param numEntries Number of sliding windows in the reference contig
   * @param fpr False Positive Rate for the BloomFilter
   * @param numHashes Number of hashes for the BloomFilter
   * @param width Bit width of the BloomFilter
   * @param winSize Size of the sliding window
   * @param contigName Contig name
   * @param fragment The initial fragment
   * @return a ContaminationModel
   */
  private def internalBuild(numEntries: Int, fpr: Double, numHashes: Int, width: Int, winSize: Int,
    path: String, contigName: String, fragment: String) = {

    val bfm = new BloomFilterMonoid(numHashes, width, defaultSeed)
    val m = Map(contigName -> FilterContext(contigName, path, numEntries, fpr, bfm.create(fragment)))
    new ContaminationModel(winSize, m)
  }
}

/**
 * Created by chris-zen on 11/07/2015.
 */
class ContaminationModel(val winSize: Int, val filters: Map[String, FilterContext]) {

  def ++(model: ContaminationModel): ContaminationModel = {
    require(winSize == model.winSize, "It is not possible to merge models with different window size")

    val contigNames = filters.keySet ++ model.filters.keySet
    val m = contigNames.map({ name =>
      if (!filters.contains(name))
        name -> model.filters(name)
      else if (!model.filters.contains(name))
        name -> filters(name)
      else {
        val filterContext = filters(name)
        name -> filterContext.copy(
          bloomFilter = filterContext.bloomFilter ++ model.filters(name).bloomFilter)
      }
    }).toMap
    new ContaminationModel(winSize, m)
  }

  def filterPositives(reads: RDD[AlignmentRecord]): (RDD[AlignmentRecord], ContaminationStats) = {

    val numReads = reads.cache().count().toInt

    // TODO this should be broadcasted
    val filtersValues = filters.values

    val positiveReads = reads.flatMap({ read =>
      val seq = read.getSequence
      require(seq.size == winSize, "Reads length should be equal to the model sliding window size")

      var hasPositive = false
      val it = filtersValues.iterator
      while (it.hasNext && !hasPositive)
        hasPositive = it.next().bloomFilter.contains(seq).isTrue

      if (hasPositive)
        Some(read)
      else
        None
    }).cache()

    val numPositiveReads = positiveReads.count().toInt

    val numNegativeReads = numReads - numPositiveReads
    val stats = ContaminationStats(numReads, numNegativeReads, numNegativeReads.toDouble / numReads)

    (positiveReads, stats)
  }

  def filterNegatives(reads: RDD[AlignmentRecord]): (RDD[AlignmentRecord], ContaminationStats) = {

    val numReads = reads.cache().count().toInt

    // TODO this should be broadcasted
    val filtersValues = filters.values

    val negativeReads = reads.flatMap({ read =>
      val seq = read.getSequence
      require(seq.size == winSize, "Reads length should be equal to the model sliding window size")

      val numPositives = filtersValues.map(filterContext => if (filterContext.bloomFilter.contains(seq).isTrue) 1 else 0).sum

      if (numPositives == 0)
        Some(read)
      else
        None
    }).cache()

    val numNegativeReads = negativeReads.count().toInt

    val stats = ContaminationStats(numReads, numNegativeReads, numNegativeReads.toDouble / numReads)

    (negativeReads, stats)
  }

  def stats(reads: RDD[AlignmentRecord]): ContaminationStats = {
    val numReads = reads.cache().count().toInt

    // TODO this should be broadcasted
    val filtersValues = filters.values

    val numPositiveReads = reads.map({ read =>
      val seq = read.getSequence
      require(seq.size == winSize, "Reads length should be equal to the model sliding window size")

      var hasPositive = false
      val it = filtersValues.iterator
      while (it.hasNext && !hasPositive)
        hasPositive = it.next().bloomFilter.contains(seq).isTrue

      if (hasPositive) 1 else 0
    }).sum().toInt

    val numNegativeReads = numReads - numPositiveReads
    ContaminationStats(numReads, numNegativeReads, numNegativeReads.toDouble / numReads)
  }
}
