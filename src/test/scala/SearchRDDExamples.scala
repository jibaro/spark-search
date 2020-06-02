/*
 *    Copyright 2020 the Spark Search contributors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, computers
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

import java.io.File
import java.net.URL

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.spark.search.rdd._
import org.apache.spark.sql.SparkSession

import scala.sys.process._

import scala.collection.JavaConverters._

/**
 * Spark Search RDD examples.
 */
object SearchRDDExamples {

  case class Review(asin: String, helpful: Array[Long], overall: Double,
                    reviewText: String, reviewTime: String, reviewerID: String,
                    reviewerName: String, summary: String, unixReviewTime: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Spark Search Examples").getOrCreate()
    import spark.implicits._

    // Amazon computers reviews
    println("Downloading amazon computers reviews file...")
    val computersReviewFile = File.createTempFile("reviews_Computers", ".json.gz")
    computersReviewFile.deleteOnExit()
    new URL("http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Computers.json.gz") #> computersReviewFile !!

    println("Amazon computers reviews file downloaded, loading...")

    // Amazon computers review
    // Number of partition is the number of Lucene index which will be created
    val computersReviewsRDD = spark.read.json(computersReviewFile.getAbsolutePath).as[Review].rdd.cache
      // Number of partition is the number of Lucene index which will be created across your cluster
      .repartition(4)
    println(s"${computersReviewsRDD.count} amazon computers reviews loaded, indexing...")

    // Search RDD API
    // import org.apache.spark.search.rdd._ to implicitly enhance RDD with search features

    // Count positive review: indexation + count matched doc
    val happyReview = computersReviewsRDD.count("reviewText:happy OR reviewText:best or reviewText:good")
    println(s"${happyReview} positive reviews :)")

    // Search for key words
    println(s"Full text search results:")
    computersReviewsRDD.searchList("reviewText:\"World of Warcraft\" OR reviewText:\"Civilization IV\"", 100)
      .foreach(println)

    // /!\ Important lucene indexation is done each time a SearchRDD is computed,
    // if you do multiple operations on the same parent RDD, you might a variable in the driver:
    val computersReviewsSearchRDD = computersReviewsRDD.searchRDD(
      SearchRDDOptions.builder[Review]() // See all other options SearchRDDOptions, IndexationOptions and ReaderOptions
        .readerOptions(ReaderOptions.builder()
          .defaultFieldName("reviewText")
          .build())
        .analyzer(classOf[EnglishAnalyzer])
        .build())
    println("All reviews speaking about hardware:")
    computersReviewsSearchRDD.searchList("(RAM or memory) and (CPU or processor)^4", 10).foreach(println)

    // Fuzzy matching
    println("Some typo in names:")
    computersReviewsSearchRDD.search("reviewerName:Mikey~0.8 or reviewerName:Wiliam~0.4 or reviewerName:jonh~0.2", 100)
      .map(doc => (doc.getSource.reviewerName, doc.getScore))
      .foreach(println)

    // Amazon software reviews
    println("Downloading amazon software reviews file...")
    val softwareReviewsFile = File.createTempFile("reviews_Software", ".json.gz")
    softwareReviewsFile.deleteOnExit()
    new URL("http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Software_10.json.gz") #> softwareReviewsFile !!
    val softwareReviewsRDD = spark.read.json(softwareReviewsFile.getAbsolutePath).as[Review].rdd.cache.filter(_.reviewerName != null)

    println("Downloaded amazon software reviews file, matching reviewer against computers:")
    // Match software and computer reviewers
    val matchesReviewersRDD = computersReviewsSearchRDD.searchJoin(softwareReviewsRDD,
      (sr: Review) => s"reviewerName:${"\"" + sr.reviewerName.replace('"', ' ') + "\""}~8", 10)
    matchesReviewersRDD
      .filter(!_.getHits.isEmpty)
      .map(m => (m.doc.reviewerName, m.hits.asScala.map(h => (h.source.reviewerName, h.score))))
      .foreach(println)

    spark.close()
  }
}
