package contadamination.results

import com.twitter.algebird.{BloomFilterMonoid, BF}
import org.scalatest.{Assertions, FunSuite}

/**
 * Created by dahljo on 7/10/15.
 */
class ContaminationFilterUtilsTest extends FunSuite with Assertions {


  val windowSize = 3

  val bloomFilterCreater =
    BloomFilterMonoid(6, 10000, 1)
  val bloomFilterX = bloomFilterCreater.create("AAA")
  val bloomFilterY = bloomFilterCreater.create("TTT")
  val contaminationFilterX = ContaminationFilter(bloomFilterX, "test_organism", totalNbrOfQueries = 1, hits = 2)
  val contaminationFilterY = ContaminationFilter(bloomFilterY, "test_organism", totalNbrOfQueries = 3, hits = 5)


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
    assert(result.hits == 7)
    assert(result.totalNbrOfQueries == 4)
  }

}
