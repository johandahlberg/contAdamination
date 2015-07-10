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

  test("testContaminationRate") {
    assert(contaminationFilterX.contaminationRate === 0.5)
  }

  test("testQuery") {

    val queryForTrippleA = contaminationFilterX.query("AAA")
    assert(queryForTrippleA.hits === contaminationFilterX.hits + 1)
    assert(queryForTrippleA.totalNbrOfQueries === contaminationFilterX.totalNbrOfQueries + 1)

    val queryForTrippleT = contaminationFilterX.query("TTT")
    assert(queryForTrippleT.hits === contaminationFilterX.hits)
    assert(queryForTrippleT.totalNbrOfQueries === contaminationFilterX.totalNbrOfQueries + 1)
  }

  test("testToString") {
    pending
  }

}
