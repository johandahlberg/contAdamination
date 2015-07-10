package contadamination

import java.io.File

import contadamination.bloom.BloomFilterBuilder
import contadamination.results.{ ContaminationFilterUtils, ContaminationFilter }
import org.apache.spark.{ SparkConf, SparkContext }
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.AlignmentRecord

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

  val seqOp =
    (filters: Array[ContaminationFilter], read: AlignmentRecord) =>
      ContaminationFilterUtils.seqOp(windowSize)(filters, read.getSequence)

  val combOp =
    (x: Array[ContaminationFilter], y: Array[ContaminationFilter]) =>
      ContaminationFilterUtils.combOp(x, y)

  val results = reads.
    aggregate(contaminationFilters)(seqOp, combOp)

  results.foreach(println)
}
