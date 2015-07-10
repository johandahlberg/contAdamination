package contadamination.results

import com.twitter.algebird.{BloomFilterMonoid}
import contadamination.test.utils.ContadaminationSuite

/**
 * Created by dahljo on 7/10/15.
 */
class ContaminationFilterUtilsTest extends ContadaminationSuite {


  val windowSize = 3

  val bloomFilterCreater =
    BloomFilterMonoid(6, 10000, 1)
  val bloomFilterX = bloomFilterCreater.create("AAA")
  val bloomFilterY = bloomFilterCreater.create("TTT")
  val contaminationFilterX = ContaminationFilter(bloomFilterX, "test_organism", totalNbrOfQueries = 2, hits = 1)
  val contaminationFilterY = ContaminationFilter(bloomFilterY, "test_organism", totalNbrOfQueries = 5, hits = 3)


  test("testSeqOp") {
    val filters = Array(contaminationFilterX)
    val read = "AAA"
    val result = ContaminationFilterUtils.seqOp(windowSize)(filters, read)
    assert(result(0).hits === contaminationFilterX.hits + 1)
  }

  test("testCombOp") {
    val firstFilter = Array(contaminationFilterX)
    val secondFilter = Array(contaminationFilterY)

    val result = ContaminationFilterUtils.combOp(firstFilter, secondFilter)
    assert(result(0).hits === contaminationFilterX.hits + contaminationFilterY.hits)
    assert(result(0).totalNbrOfQueries ===
      contaminationFilterX.totalNbrOfQueries + contaminationFilterY.totalNbrOfQueries)
  }

  test("testAdd") {
    val result = ContaminationFilterUtils.add(contaminationFilterX, contaminationFilterY)
    assert(result.hits == 4)
    assert(result.totalNbrOfQueries == 7)
  }

}
