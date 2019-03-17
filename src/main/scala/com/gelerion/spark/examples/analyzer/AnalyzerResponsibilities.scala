package com.gelerion.spark.examples.analyzer

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
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
    // ResolveReferences + ResolveRelations
    resolveUnresolvedAttributeAndRelation()
  }

  // ResolveReferences + ResolveRelations
  def resolveUnresolvedAttributeAndRelation() = {
    // select attribute from table
    val projection = logical.Project(Seq(UnresolvedAttribute("attribute_str")), UnresolvedRelation(None, "table"))
    println(analyzer(projection))

  }

}
