package com.gelerion.spark.examples.optimizer

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.optimizer.{CombineLimits, ConstantFolding, NullPropagation}
import org.apache.spark.sql.catalyst.plans.logical.{Limit, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.types.IntegerType

object OptimizerResponsibilities {

  val catalog: Catalog   = new SimpleCatalog(true)
  val analyzer: Analyzer = new Analyzer(catalog, EmptyFunctionRegistry, caseSensitive = true)

  /*
  Optimizer is responsible for:
   1. Combine Limits [combines two adjacent Limit operators into one, merging the expressions into one single expression]
   2. Constant Folding [replaces Expressions that can be statically evaluated with Literal values]
        - NullPropagation
        - ConstantFolding
        - LikeSimplification
        - BooleanSimplification
        - SimplifyFilters
        - SimplifyCasts
        - SimplifyCaseConversionExpressions
   3. Filter Pushdown [attempts to eliminate the reading of unneeded columns from the query plan]
        - CombineFilters
        - PushPredicateThroughProject
        - PushPredicateThroughJoin
        - ColumnPruning
   */

  def main(args: Array[String]): Unit = {
    // select attribute from table [limit 10 limit 5] => limit 5
    combineLimits()

    constantsFolding()
  }

  def constantsFolding() = {

  }

  def combineLimits() = {
    //SELECT attribute FROM table LIMIT 10 LIMIT 5
    println("When several limits are defined the lowest one would be chosen: SELECT * FROM table LIMIT 10 LIMIT 5 => SELECT * FROM table LIMIT 5")
    println("----------------------------------------------------")
    val relation = LocalRelation(AttributeReference("attribute", IntegerType, nullable = true)(/*generate exprId*/))

    /* relation.select('a).limit(10).limit(5) */
    val select = Project(Seq(UnresolvedAttribute("attribute")), relation)

    val limit_10 = Limit(Literal(10), select)
    val limit_5  = Limit(Literal(5), limit_10)
    var analyzed = analyzer(limit_5)
    println(s"Before optimizing:\n $analyzed")

    println(
      """
        |Limits are folded in several steps:
        | 1. Limit 5 Limit 10 are merged into one foldable expression: Limit If ((10 < 5)) 10 else 5
        |    If more than one limit is present they would be folded as nested ones:
        |       Limit If ((10 < if ((5 < 8)) 5 else 8)) 10 else if ((5 < 8)) 5 else 8
        |
        | 2. If Expression isn't depended on relation data, hence is foldable.
        |    ConstantFolding replaces expressions that can be statically evaluated with equivalent Literal values
        |       If ((10 < 5)) 10 else 5 -> If (false) 5 else 8
        |
        | 3. Then NullPropagation kicks in and rewrites if (false) 5 else 8 according to the following rule
        |    if (v == true) trueValue else falseValue
        |"""
      .stripMargin)

    var optimized = CombineOnlyLimitsOptimizer(analyzed)
    println(s"Combine only optimizing:\n $optimized")

    optimized = CombineLimitsThenFoldOptimizer(analyzed)
    println(s"Combine then fold optimizing:\n $optimized")

    optimized = CombineLimitsThenFoldWithNullPropagationOptimizer(analyzed)
    println(s"Combine then folded and null propagated optimizing:\n $optimized")

    println("Three limits optimization example: SELECT * FROM table LIMIT 10 LIMIT 5 LIMIT 8 => SELECT * FROM table LIMIT 5")
    println("----------------------------------------------------")
    val limit_8 = Limit(Literal(8), limit_5)
    /* relation.select('a).limit(10).limit(5).limit(8) */
    analyzed = analyzer(limit_8)

    optimized = CombineOnlyLimitsOptimizer(analyzed)
    println(s"Combine only optimizing:\n $optimized")

    optimized = CombineLimitsThenFoldOptimizer(analyzed)
    println(s"Combine then fold optimizing:\n $optimized")

    optimized = CombineLimitsThenFoldWithNullPropagationOptimizer(analyzed)
    println(s"Combine then folded and null propagated optimizing:\n $optimized")
  }
}

object CombineOnlyLimitsOptimizer extends RuleExecutor[LogicalPlan] {
  override val batches: Seq[CombineOnlyLimitsOptimizer.Batch] =
    Batch("Combine Limits", FixedPoint(2), CombineLimits) :: Nil
}

object CombineLimitsThenFoldOptimizer extends RuleExecutor[LogicalPlan] {
  override val batches: Seq[CombineLimitsThenFoldOptimizer.Batch] =
    Batch("Combine Limits", FixedPoint(2), CombineLimits) ::
    Batch("Constant Folding", FixedPoint(3), ConstantFolding) ::
    Nil
}

object CombineLimitsThenFoldWithNullPropagationOptimizer extends RuleExecutor[LogicalPlan] {
  override val batches: Seq[CombineLimitsThenFoldWithNullPropagationOptimizer.Batch] =
    Batch("Combine Limits", FixedPoint(2), CombineLimits) ::
    Batch("Constant Folding", FixedPoint(3), NullPropagation, ConstantFolding) ::
    Nil
}