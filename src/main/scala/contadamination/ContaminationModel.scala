package contadamination

import java.io.File

import com.twitter.algebird.{ BloomFilterMonoid, BloomFilter, BF }
import contadamination.ContaminationModel.{ ContaminationStats, FilterContext }
import org.apache.spark.mllib.rdd.RDDFunctions
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.{NucleotideContigFragment, AlignmentRecord}
import org.bdgenomics.adam.rdd.ADAMContext._

object ContaminationModel {

  val defaultKmerSize = 32
  val defaultFpr = 0.1
  val defaultSeed = 0

  /**
   * The contamination BloomFilter and it's context.
   * @param generatedFromFile File name that the filter was generated from used as identifier.
   * @param path Reference path
   * @param numEntries Estimated number of entires to hold
   * @param fpr False positive rate
   * @param bloomFilter Bloom filter
   */
  case class FilterContext(generatedFromFile: String,
                           path: String,
                           numEntries: Int,
                           fpr: Double,
                           bloomFilter: BF)

  /**
   * Statistics on queries against a contamination bloom filter.
   * @param numReads  Total number of reads checked
   * @param numNegatives True Negatives (number of negative reads)
   * @param rate Contamination rate as the number of negative reads over the total number of reads
   */
  case class ContaminationStats(numReads: Long, numNegatives: Long, rate: Double)

  /**
   * Build a ContaminationModel from a sequence of paths
   * @param paths Sequence of FASTA files for the reference contigs
   * @param kmerSize Sliding window size
   * @param fpr False Positive Rate
   * @param adamContext ADAM Context
   * @return a ContaminationModel
   */
  def apply(paths: Seq[String], kmerSize: Int = defaultKmerSize, fpr: Double = defaultFpr)(implicit adamContext: ADAMContext): ContaminationModel = {
    paths.map(ContaminationModel(_, kmerSize, fpr)).reduce((x, y) => x ++ y)
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

    val fragments = adamContext.loadSequence(path)
    val fileName = new File(path).getName

    require(fragments.count() > 0, "At least one fragment is required")

    val kmersAndCounts = fragments.countKmers(winSize).cache()

    val numEntries = kmersAndCounts.count()
    require(numEntries > Int.MaxValue,
      "Number of k-mers was larger than IntMax, the current implementation can't handle that.")
    val numEntriesAsInt = numEntries.toInt

    // TODO check that the cost of creating a new bloomfilter per fragment is not too high
    val width = BloomFilter.optimalWidth(numEntriesAsInt, fpr)
    val numHashes = BloomFilter.optimalNumHashes(numEntriesAsInt, width)
    kmersAndCounts
      .map {
      case (kMer: String, _) =>
        ContaminationModel.internalBuild(numEntriesAsInt, fpr, numHashes, width, winSize, path, kMer, fileName)
    }.reduce((x, y) => x ++ y)
  }

  /**
   * Internal builder for a ContaminationModel from a single fragment.
   * @param numEntries Number of sliding windows in the reference contig
   * @param fpr False Positive Rate for the BloomFilter
   * @param numHashes Number of hashes for the BloomFilter
   * @param width Bit width of the BloomFilter
   * @param kmerSize Size of the sliding window
   * @param kMer The initial fragment
   * @param fileName Name of the file the filter was generated from.
   * @return a ContaminationModel
   */
  private def internalBuild(numEntries: Int, fpr: Double, numHashes: Int, width: Int, kmerSize: Int,
                            path: String, kMer: String, fileName: String) = {

    val bfm = new BloomFilterMonoid(numHashes, width, defaultSeed)
    val filterContext = FilterContext(fileName, path, numEntries, fpr, bfm.create(kMer))
    new ContaminationModel(kmerSize, filterContext)
  }
}

/**
 * Created by chris-zen on 11/07/2015.
 */
class ContaminationModel(val kMerSize: Int, val filter: FilterContext) {

  private def kmersFromRead(read: String, kmerSize: Int): Iterator[String] =
    read.iterator.sliding(kmerSize).withPartial(false).map(_.mkString)

  def ++(model: ContaminationModel): ContaminationModel = {
    require(this.kMerSize == model.kMerSize,
      "It is not possible to merge models with different window size")
    require(this.filter.generatedFromFile == model.filter.generatedFromFile,
      "Not possible to add two models generated from different files.")

    val addedFilters = filter.copy(bloomFilter = this.filter.bloomFilter ++ model.filter.bloomFilter)

    new ContaminationModel(kMerSize, addedFilters)
  }

  private def generalFilter(reads: RDD[AlignmentRecord])(kmerFilterCriteria: String => Boolean): RDD[AlignmentRecord] = {

    val matchingReads = reads.flatMap({ read =>
      val seq = read.getSequence

      val kmersInRead = kmersFromRead(seq, this.kMerSize)
      val isAHit = kmersInRead.exists(kmer => kmerFilterCriteria(kmer))

      if (isAHit)
        Some(read)
      else
        None
    }).cache()

    matchingReads
  }

  private def positiveReadCriteria(s: String): Boolean =
    this.filter.bloomFilter.contains(s).isTrue

  private def negativeReadCriteria(s: String): Boolean =
    !this.filter.bloomFilter.contains(s).isTrue

  def filterPositives(reads: RDD[AlignmentRecord]): (RDD[AlignmentRecord], ContaminationStats) = {

    val numReads = reads.cache().count()
    val positiveReads = generalFilter(reads)(positiveReadCriteria)
    val numPositiveReads = positiveReads.count()
    val numNegativeReads = numReads - numPositiveReads
    val stats = ContaminationStats(numReads, numNegativeReads, numNegativeReads.toDouble / numReads)

    (positiveReads, stats)
  }

  def filterNegatives(reads: RDD[AlignmentRecord]): (RDD[AlignmentRecord], ContaminationStats) = {

    val numReads = reads.cache().count()
    val negativeReads = generalFilter(reads)(negativeReadCriteria)
    val numNegativeReads = negativeReads.count()
    val stats = ContaminationStats(numReads, numNegativeReads, numNegativeReads.toDouble / numReads)

    (negativeReads, stats)
  }

  def stats(reads: RDD[AlignmentRecord]): ContaminationStats = {
    val numReads = reads.cache().count()

    val numPositiveReads =
      generalFilter(reads)(positiveReadCriteria).count()
    val numNegativeReads = numReads - numPositiveReads

    ContaminationStats(numReads, numNegativeReads, numNegativeReads.toDouble / numReads)
  }
}
