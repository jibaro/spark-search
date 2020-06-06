package org.apache.spark.search.sql

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan}

case class SearchJoinLogicalPlan(left: LogicalPlan,
                                 right: SearchRDDLogicalPlan,
                                 conditions: Expression)
  extends BinaryNode with PredicateHelper {

  override def output: Seq[Attribute] = left.output
}

case class SearchRDDLogicalPlan(child: LogicalPlan)
  extends LeafNode {

  // FIXME take only indexed fields
  override def output: Seq[Attribute] = child.schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
}
