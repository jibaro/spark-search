package org.apache.spark.search.sql

import org.apache.lucene.util.QueryBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.search.rdd.SearchRDD
import org.apache.spark.search.{IndexationOptions, ReaderOptions, SearchOptions, _}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

case class SearchJoinPhysicalPlan(left: SparkPlan,
                                  right: SparkPlan,
                                  conditions: Expression)
  extends BinaryExecNode {
  // FIXME support left val retrieving from other rows / support code gen
  override protected def doExecute(): RDD[InternalRow] = {

    val leftRDD = left.execute()
    val searchRDD = right.execute().asInstanceOf[SearchRDD[InternalRow]]
    val opts = searchRDD.options

    val qb = conditions match {
      case MatchesExpression(left, right, includeScore) => right match {
        case Literal(value, dataType) =>
          dataType match {
            case StringType => left match {
              case a: AttributeReference =>
                queryBuilder[InternalRow]((r: InternalRow, lqb: QueryBuilder) =>
                  lqb.createPhraseQuery(a.name, value.asInstanceOf[UTF8String].toString), opts)
              case _ => throw new UnsupportedOperationException
            }
            case _ => throw new UnsupportedOperationException
          }
        case _ => throw new UnsupportedOperationException
      }
      case _ => throw new IllegalArgumentException
    }

    val rdd = searchRDD.searchQueryJoin(leftRDD, qb, 1)
      .map(m => m.doc)

    rdd.foreach(println)

    rdd
  }

  override def output: Seq[Attribute] = left.output ++ right.output
}


case class SearchRDDPhysicalPlan(child: SparkPlan)
  extends UnaryExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDDs = child.asInstanceOf[WholeStageCodegenExec].child.asInstanceOf[CodegenSupport].inputRDDs()

    if (inputRDDs.length > 1) {
      throw new UnsupportedOperationException("multiple RDD not supported")
    }

    val rdd = inputRDDs.head

    // FIXME index only necessaries columns
    val schema = child.schema

    val opts = SearchOptions.builder[InternalRow]()
      .read((readOptsBuilder: ReaderOptions.Builder[InternalRow]) => readOptsBuilder.documentConverter(new DocumentRowConverter(schema)))
      .index((indexOptsBuilder: IndexationOptions.Builder[InternalRow]) => indexOptsBuilder.documentUpdater(new DocumentRowUpdater(schema)))
      .build()

    new SearchRDD[InternalRow](rdd, opts)
  }

  override def output: Seq[Attribute] = Seq.empty // FIXME add score here
}