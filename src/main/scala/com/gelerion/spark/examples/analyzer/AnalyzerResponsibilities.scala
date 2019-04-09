package com.gelerion.spark.examples.analyzer

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Count}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.{IntegerType, StringType}

object AnalyzerResponsibilities {

  val catalog: Catalog   = new SimpleCatalog(true)
  val analyzer: Analyzer = new Analyzer(catalog, EmptyFunctionRegistry, caseSensitive = true)

  //relation must be registered
  catalog.registerTable(None, "table",
    LocalRelation(Seq(
      AttributeReference("attribute_str", StringType) (/*generate exprId*/),
      AttributeReference("attribute_int", IntegerType)(/*generate exprId*/)
    )))


  /*
  Analyzer is responsible for:
   1. ResolveReferences (UnresolvedAttribute)
   2. ResolveRelations (UnresolvedRelation, lookup from Catalog)
   3. StarExpansion (* to set of expressions)
   4. ResolveFunctions (UnresolvedFunction, lookup from FunctionsRegistry)
   5. GlobalAggregates
  */

  def main(args: Array[String]): Unit = {
    // ResolveReferences + ResolveRelations [SELECT attribute_str FROM table]
    resolveUnresolvedAttributeAndRelation()

    // StarExpansion [SELECT * FROM table]
    starExpansion()

    // GlobalAggregates [SELECT count(attribute_str) FROM table GROUP BY attribute_str]
    globalAggregate()
  }

  // ResolveReferences + ResolveRelations
  def resolveUnresolvedAttributeAndRelation() = {
    // SELECT attribute FROM table
    println("Resolve UnresolvedAttributes and UnresolvedRelations: SELECT attribute FROM table")
    println("----------------------------------------------------")
    val projection = logical.Project(Seq(UnresolvedAttribute("attribute_str")), UnresolvedRelation(None, "table"))
    println(s"Before analyzing:\n $projection")
    // Project("attribute_str", LocalRelation("test))
    println(s"After analyzing:\n ${analyzer(projection)}")
  }

  def starExpansion() = {
    // SELECT * FROM table
    println("Star expansion: SELECT * FROM table")
    println("----------------------------------------------------")
    val projection = logical.Project(Seq(Star(None)), UnresolvedRelation(None, "table"))
    println(s"Before analyzing:\n $projection")
    // Project("attribute_str, attribute_int", LocalRelation("test))
    println(s"After analyzing:\n ${analyzer(projection)}")

  }

  def globalAggregate() = {
    // SELECT count(attribute_str) FROM table GROUP BY attribute_str
    println("Transforms aggregate expression in select query into Aggregate plan: SELECT count(attribute_str) FROM table")
    println("----------------------------------------------------")
    val projections = Alias(Count(UnresolvedAttribute("attribute_str")), "c1")(/*generate exprId*/)
    val from        = UnresolvedRelation(None, "table")
    val groupBy     = Alias(UnresolvedAttribute("attribute_str"), "c0")(/*generate exprId*/)
//    Aggregate(Seq(groupBy), Seq(projections), from)

    val projection = logical.Project(Seq(projections), from)
    println(s"Before analyzing:\n $projection")
    // turns an above into:
    //   Aggregate(Nil, Seq(projections), from)

    println(s"After analyzing:\n ${analyzer(projection)}")
  }
}
