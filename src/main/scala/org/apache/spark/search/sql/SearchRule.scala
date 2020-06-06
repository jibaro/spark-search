package org.apache.spark.search.sql

import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 *
 *
 * @author Pierrick HYMBERT
 */
object SearchRule extends Rule[LogicalPlan] {
  override def apply(executionPlan: LogicalPlan): LogicalPlan = {
    executionPlan match {
      case agg@Aggregate(groupingExpressions, aggregateExpressions, aggregateChild) =>
        val (isRewritten, rewritten) = rewriteForSearch(aggregateChild)
        if (isRewritten) Aggregate(groupingExpressions, aggregateExpressions, rewritten) else agg
      case _ => executionPlan
    }
  }

  private def rewriteForSearch(aggregateChild: LogicalPlan): (Boolean, LogicalPlan) = {
    aggregateChild match {
      case project@Project(projectList, child) => child match {
        case Filter(conditions, childFilter) =>
          val (searchExpression, newConditions) = searchConditions(conditions)
          searchExpression.map(searchExpression => {
            val rddPlan = SearchRDDLogicalPlan(project)
            val joinPlan = SearchJoinLogicalPlan(Project(projectList, Filter(newConditions, childFilter)), rddPlan, searchExpression)
            (true, joinPlan)
          }).getOrElse((false, project))
        case _ => (false, project)
      }
      case _ => (false, aggregateChild)
    }
  }

  private def searchConditions(condition: Expression): (Option[Expression], Expression) = {
    condition match {
      case And(left, right) =>
        right match {
          case s: MatchesExpression =>
            (Option(s), left)
          case _ => (Option.empty[Expression], condition)
        }
      case _ => (Option.empty[Expression], condition)
    }
  }
}
