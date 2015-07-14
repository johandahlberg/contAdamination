package contadamination.results

import com.twitter.algebird.BloomFilterMonoid
import contadamination.test.utils.ContadaminationSuite

/**
 * Created by dahljo on 7/10/15.
 */
class ContaminationFilterTest extends ContadaminationSuite {

  val bloomFilterCreater =
    BloomFilterMonoid(6, 10000, 1)
  val bloomFilterX = bloomFilterCreater.create("AAA")
  val contaminationFilterX = ContaminationFilter(bloomFilterX, "test_organism", totalNbrOfQueries = 2, hits = 1)

  behavior of "ContaminationModel"

  it should "compute a correct contamination rate" in {
    assert(contaminationFilterX.contaminationRate === 0.5)
  }

  it should "be possible to query the filter" in {
    val queryForTrippleA = contaminationFilterX.query("AAA")
    assert(queryForTrippleA.hits === contaminationFilterX.hits + 1)
    assert(queryForTrippleA.totalNbrOfQueries === contaminationFilterX.totalNbrOfQueries + 1)

    val queryForTrippleT = contaminationFilterX.query("TTT")
    assert(queryForTrippleT.hits === contaminationFilterX.hits)
    assert(queryForTrippleT.totalNbrOfQueries === contaminationFilterX.totalNbrOfQueries + 1)
  }
}
