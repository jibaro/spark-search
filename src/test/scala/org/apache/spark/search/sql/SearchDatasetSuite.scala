package org.apache.spark.search.sql

import org.apache.spark.search._
import org.scalatest.funsuite.AnyFunSuite

class SearchDatasetSuite extends AnyFunSuite with LocalSparkSession {


  test("A column can be searchable") {
    val spark = _spark
    import spark.sqlContext.implicits._

    val companies = TestData.companiesDS(spark).repartition(4).cache

    val ibmCompany = companies.where($"name".matches("ibm"))

    ibmCompany.select($"name").show(1000)

    assertResult(1)(ibmCompany.count)
  }
}
