package org.apache.spark.search.sql

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * A dataset with search features
 *
 * @author Pierrick HYMBERT
 */
class DatasetWithSearch[T](dataset: Dataset[T]) {
  def search(): Dataset[T] = {
    if (dataset.sparkSession.experimental.extraStrategies.contains(_searchStrategy))
      dataset.sparkSession.experimental.extraStrategies += _searchStrategy
    dataset
  }
}
