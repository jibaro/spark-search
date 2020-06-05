package org.apache.spark.search.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

/**
 * Search strategy.
 *
 * @author Pierrick HYMBERT
 */
class SearchStrategy() extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan
  }
}
