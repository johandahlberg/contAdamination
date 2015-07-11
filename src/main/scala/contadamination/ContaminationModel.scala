package contadamination

import com.twitter.algebird.BF
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord

/**
 * Created by chris-zen on 11/07/2015.
 */
class ContaminationModel(val winSize: Int, val filters: Map[String, BF]) {

  case class ContaminationStats(
               numReads: Int, // Total number of reads checked
               tn: Int,       // True Negatives (number of rejected reads)
               rate: Double   // Contamination rate as the number of rejected reads over the total number of reads
  )
  
  def filter(reads: RDD[AlignmentRecord]): (RDD[AlignmentRecord], Map[String, ContaminationStats]) = ???
  
  def stats(reads: RDD[AlignmentRecord]): Map[String, ContaminationStats] = ???
}
