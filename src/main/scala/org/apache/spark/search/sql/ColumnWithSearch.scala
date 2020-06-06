package org.apache.spark.search.sql

import org.apache.spark.search.SearchException
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

/**
 *
 * @author Pierrick HYMBERT
 */
class ColumnWithSearch(col: Column) {
  @transient private final val sqlContext = SparkSession.getActiveSession.map(_.sqlContext).orNull

  def matches(literal: String): Column = withSearchExpr {
    MatchesExpression(col.expr, lit(literal).expr, includeScore = false)
  }

  def matches(col: Column): Column = withSearchExpr {
    MatchesExpression(col.expr, col.expr, includeScore = false)
  }

  def score(): Column = withSearchExpr {
    col.expr match {
      case MatchesExpression(left, right, _) => MatchesExpression(left, right, includeScore = true)
      case _ => throw new SearchException(
        "scoring on a column not supported by spark search is not allowed, call matches first")
    }
  }

  /** Creates a column based on the given expression. */
  private def withSearchExpr(newExpr: Expression): Column = {
    val searchSQLEnabled = sqlContext.experimental.extraStrategies.contains(SearchStrategy)

    if (!searchSQLEnabled) {
      sqlContext.experimental.extraStrategies = Seq(SearchStrategy) ++ sqlContext.experimental.extraStrategies
      sqlContext.experimental.extraOptimizations = Seq(SearchRule) ++ sqlContext.experimental.extraOptimizations
    }

    new Column(newExpr)
  }
}
