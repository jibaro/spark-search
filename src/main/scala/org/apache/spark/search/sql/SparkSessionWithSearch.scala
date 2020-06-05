package org.apache.spark.search.sql

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author Pierrick HYMBERT
 */
class SparkSessionWithSearch(sparkSession: SparkSession) {
  def enableSearch(): Unit = {
    sparkSession.experimental.extraStrategies += new SearchStrategy()
  }
}
