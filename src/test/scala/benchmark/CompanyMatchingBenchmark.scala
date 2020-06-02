/*
 *    Copyright 2020 the Spark Search contributors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package benchmark

import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper
import org.apache.spark.search.rdd._
import org.apache.spark.search.sql._
import org.apache.spark.sql.SparkSession

case class SecEdgarCompanyInfo(lineNumber: String, companyName: String, CompanyCIKKey: String)

case class AnyCompany(id: String, name: String, domain: String, yearFounded: String,
                      industry: String, sizeRange: String, locality: String,
                      country: String, linkedinUrl: String,
                      currentEmployeeEstimate: String, totalEmployeeEstimate: String)

object CompanyMatchingBenchmark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Spark Search Benchmark").getOrCreate()

    import spark.implicits._

    // https://www.kaggle.com/peopledatalabssf/free-7-million-company-dataset
    val companies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))
      .withColumnRenamed("_c0", "id")
      .withColumnRenamed("year founded", "yearFounded")
      .withColumnRenamed("size range", "sizeRange")
      .withColumnRenamed("linkedin url", "linkedinUrl")
      .withColumnRenamed("current employee estimate", "currentEmployeeEstimate")
      .withColumnRenamed("total employee estimate", "totalEmployeeEstimate")
      .as[AnyCompany].rdd.cache

    // https://www.kaggle.com/dattapiy/sec-edgar-companies-list
    val secEdgarCompanyRDD = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(args(1))
      .withColumnRenamed("Line Number", "lineNumber")
      .withColumnRenamed("Company Name", "companyName")
      .withColumnRenamed("Company CIK Key", "companyCIKKey")
      .as[SecEdgarCompanyInfo].rdd.cache

    val matchedCompanies = companies.searchRDD(SearchRDDOptions
      .builder[AnyCompany]
      .analyzer(classOf[ShingleAnalyzerWrapper]).build).cache
      .matching(secEdgarCompanyRDD, (c: SecEdgarCompanyInfo) => s"name:${"\"" + c.companyName + "\""}", 1)

    matchedCompanies.toDS().write.json("output.json")

    spark.stop
  }
}
