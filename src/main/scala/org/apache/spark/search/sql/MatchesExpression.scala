/*
 *    Copyright 2020 the Spark Search contributors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.spark.search.sql


import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * @author Pierrick HYMBERT
 */
case class MatchesExpression(left: Expression, right: Expression, includeScore: Boolean)
  extends BinaryExpression
    with ImplicitCastInputTypes
    with NullIntolerant{

  override def toString: String = s"$left MATCHES $right"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // FIXME generate code to call query
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      val rightStr = ctx.freshName("rightStr")
      s"""
         | String $rightStr = $eval2.toString();
         | ${ev.value} = true; // $eval1.toString().equals($rightStr); FIXME
        """.stripMargin
    })
  }

  override def dataType: DataType = BooleanType

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)
}

