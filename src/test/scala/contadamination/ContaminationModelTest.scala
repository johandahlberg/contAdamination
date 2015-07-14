package contadamination

import contadamination.test.utils.{ContadaminationSuite, AdamTestContext}
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.NucleotideContigFragment
import org.scalatest.FlatSpec

/**
 * Created by dahljo on 7/13/15.
 */
class ContaminationModelTest extends ContadaminationSuite with AdamTestContext {

  val path = "src/test/resources/mt.fasta"
  val winSize = 10
  val falsePositiveRate = 0.01

  behavior of "ContaminationModel"

  it should "count the number of k-mers in a reference file" in {
    def estimateNumberOfEntriesFromReference(fragments: RDD[NucleotideContigFragment], winSize: Int): Int = {
      // TODO I have concerns about the high number of entries and the user asking for a low FPR
      import org.bdgenomics.adam.rdd.ADAMContext._

      val numEntries = fragments.countKmers(winSize).count()
      val numEntriesAsInt =
        if(numEntries > Int.MaxValue)
          throw new RuntimeException("Number of k-mers was larger than IntMax, the current implementation can't handle that.")
        else
          numEntries.toInt
      numEntriesAsInt
    }

    val fragments = adamContext.loadSequence(path)
    val numberOfKmers =
      estimateNumberOfEntriesFromReference(fragments = fragments, winSize = winSize)

    println(numberOfKmers)
  }

  it should "build a model from a single path" in {
    //val contaminationModel = ContaminationModel(path, winSize, falsePositiveRate)
    pending
  }

  it should "build a model from a multiple paths" in {
    pending
  }

  it should "be possible to add two contamination models" in {
    pending
  }

  it should "be possible filter positives" in {
    pending
  }

  it should "be possible get statistics" in {
    pending
  }
}
