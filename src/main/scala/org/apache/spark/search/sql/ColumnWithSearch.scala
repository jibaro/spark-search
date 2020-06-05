package org.apache.spark.search.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, Like}
import org.apache.spark.sql.functions.lit

/**
 *
 * @author Pierrick HYMBERT
 */
class ColumnWithSearch(col: Column) {

  def matches(literal: String): Column = withExpr { MatchesExpression(col.expr, lit(literal).expr) }

  /** Creates a column based on the given expression. */
  private def withExpr(newExpr: Expression): Column = new Column(newExpr)
}
