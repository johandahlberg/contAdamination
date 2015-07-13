package contadamination

import contadamination.test.utils.AdamTestContext
import org.scalatest.FlatSpec

/**
 * Created by dahljo on 7/13/15.
 */
class ContaminationModelTest extends FlatSpec with AdamTestContext {

  behavior of "ContaminationModelTest"

  it should "build a model from a single path" in {
    val path = "src/test/resources/mt.fasta"
    val winSize = 10
    val falsePositiveRate = 0.01

    val contaminationModel = ContaminationModel(path, winSize, falsePositiveRate)
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
