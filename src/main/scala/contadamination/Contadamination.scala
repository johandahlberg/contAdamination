package contadamination

import java.io.File

import contadamination.bloom.BloomFilterBuilder
import contadamination.results.{ ContaminationFilterUtils, ContaminationFilter }
import org.apache.spark.{ SparkConf, SparkContext }
import org.bdgenomics.adam.rdd.ADAMContext

/**
 * Created by dahljo on 7/9/15.
 */
object Contadamination extends App {

  val conf = new SparkConf().setAppName("ContAdamination").setMaster("local[1]")
  val sc = new SparkContext(conf)
  val adamContext = new ADAMContext(sc)

  // TODO Make configurable
  val probOfFalsePositive = 0.0005
  val windowSize = 30

  // TODO Read from commandline
  val readsPath = "2-5pM-3h_S3_L001_I2_001.fastq.bam.adam" // args(0)
  val referencePaths = Array("src/test/resources/mt.fasta") //args.drop(1)

  val reads = adamContext.loadAlignments(readsPath)

  val bloomFilterBuilder = new BloomFilterBuilder(
    adamContext,
    probOfFalsePositive,
    windowSize)

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

  val results = reads.aggregate(contaminationFilters)(ContaminationFilterUtils.seqOp, ContaminationFilterUtils.combOp)

  results.foreach(println)

  //fold over reads
  // bf collection map map read

  // for each returning bloom filter print results...0

}
