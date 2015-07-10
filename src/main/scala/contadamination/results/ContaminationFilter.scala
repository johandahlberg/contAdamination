package contadamination.results

import com.twitter.algebird.BF
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
    } yield
      filter.query(readSequence)
  }

  def combOp(x: Array[ContaminationFilter], y: Array[ContaminationFilter]) = {
    x.zip(y).map { tuple => add(tuple._1, tuple._2) }
  }

}

case class ContaminationFilter(bloomFilter: BF, organism: String, totalNbrOfQueries: Int = 0, hits: Int = 0) {

  def contaminationRate = hits.toDouble / totalNbrOfQueries

  def query(item: String): ContaminationFilter = {
    val isAHit = bloomFilter.contains(item).isTrue
    val addThis = if (isAHit) 1 else 0
    this.copy(totalNbrOfQueries = totalNbrOfQueries + 1, hits = hits + addThis)
  }

  override def toString(): String = {
    s"organism: $organism, totalNbrOfHits: $totalNbrOfQueries, hits: $hits"
  }

}
