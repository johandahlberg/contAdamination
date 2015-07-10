package contadamination.results

import java.io.File

import com.twitter.algebird.BF
import contadamination.bloom.BloomFilterBuilder
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord

/**
 * Created by dahljo on 7/9/15.
 */
object ContaminationFilterUtils {

  def add(x: ContaminationFilter, y: ContaminationFilter) =
    x.copy(totalNbrOfQueries = x.totalNbrOfQueries + y.totalNbrOfQueries, hits = x.hits + y.hits)

  def seqOp(windowSize: Int)(filters: Array[ContaminationFilter], readSequence: String): Array[ContaminationFilter] = {
    val windows = readSequence.sliding(windowSize)
    for {
      filter <- filters
      window <- windows
    } yield filter.query(readSequence)
  }

  def combOp(x: Array[ContaminationFilter], y: Array[ContaminationFilter]) = {
    x.zip(y).map { tuple => add(tuple._1, tuple._2) }
  }

  def queryReadsAgainstFilters(windowSize: Int, contaminationFilters: Array[ContaminationFilter], reads: RDD[AlignmentRecord]): Array[ContaminationFilter] = {
    val queryReadOperationWithWindowSize =
      (filters: Array[ContaminationFilter], read: AlignmentRecord) =>
        ContaminationFilterUtils.seqOp(windowSize)(filters, read.getSequence)

    val results = reads.
      aggregate(contaminationFilters)(
        queryReadOperationWithWindowSize, ContaminationFilterUtils.combOp)

    results
  }

  def createContaminationFilters(referencePaths: Array[String], bloomFilterBuilder: BloomFilterBuilder): Array[ContaminationFilter] = {
    // Collection of bloom filters
    val contaminationFilters =
      for {
        filePath <- referencePaths
      } yield {
        val file: File = new File(filePath)
        val filter = bloomFilterBuilder.createBloomFilter(file)
        new ContaminationFilter(
          bloomFilter = filter,
          organism = file.getName
        )
      }
    contaminationFilters
  }

}

case class ContaminationFilter(bloomFilter: BF, organism: String, totalNbrOfQueries: Int = 0, hits: Int = 0) {

  def contaminationRate = hits.toDouble / totalNbrOfQueries

  def query(item: String): ContaminationFilter = {
    val isAHit = bloomFilter.contains(item).isTrue
    val addThis = if (isAHit) 1 else 0
    val newContaminationFilter = this.copy(totalNbrOfQueries = this.totalNbrOfQueries + 1, hits = this.hits + addThis)
    newContaminationFilter
  }

  override def toString(): String = {

    """organism: %s
    |---------------------------------
    |totalNbrOfHits: %d, hits: %d""".
      format(organism, totalNbrOfQueries, hits).stripMargin
  }

}
