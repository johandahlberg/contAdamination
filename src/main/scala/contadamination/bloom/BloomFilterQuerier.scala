package contadamination.bloom

import com.twitter.algebird.BF

/**
 * Created by dahljo on 7/9/15.
 */
object BloomFilterQuerier {

  def query(filter: BF)(query: String): Boolean =
    filter.contains(query).isTrue

}
