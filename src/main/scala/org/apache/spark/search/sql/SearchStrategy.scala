package org.apache.spark.search.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

/**
 * Search strategy.
 *
 * @author Pierrick HYMBERT
 */
object SearchStrategy extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case p: SearchJoinLogicalPlan =>
        SearchJoinPhysicalPlan(planLater(p.left), planLater(p.right), p.conditions) :: Nil
      case p: SearchRDDLogicalPlan =>
        SearchRDDPhysicalPlan(planLater(p.child)) :: Nil
      case _ => Seq.empty
    }
  }
}
