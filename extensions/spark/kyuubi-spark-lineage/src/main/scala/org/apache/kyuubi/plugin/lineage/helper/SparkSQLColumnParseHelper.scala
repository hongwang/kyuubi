
package org.apache.kyuubi.plugin.lineage.helper

import scala.collection.mutable
import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.kyuubi.lineage.{LineageConf, SparkContextHelper}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NamedRelation, PersistedView, ViewType}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, Expression, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}

import org.apache.kyuubi.plugin.lineage.helper.SparkListenerHelper.SPARK_RUNTIME_VERSION
import org.apache.kyuubi.util.reflect.ReflectUtils._

trait ColumnParser {
  def sparkSession: SparkSession

  val SUBQUERY_COLUMN_IDENTIFIER = "__subquery__"
  val AGGREGATE_COUNT_COLUMN_IDENTIFIER = "__count__"
  val LOCAL_TABLE_IDENTIFIER = "__local__"

  def parse(plan: LogicalPlan): List[String] = {
    val inputTablesByPlan = mutable.HashSet[String]()
    val columns = extractColumns(
      plan,
      AttributeSet.empty,
      inputTablesByPlan).toList.collect {
      case attr => (attr.qualifier :+ attr.name).mkString(".")
    }

    columns.distinct
  }

  private def mergeColumns(
                                   left: AttributeSet,
                                   right: AttributeSet): AttributeSet = {
    left ++ right
  }

  private def joinColumns(
                                  parent: AttributeSet,
                                  child: AttributeSet): AttributeSet = {

    if (parent.isEmpty) child
    else {
      val childMap = child.map(attr => attr.exprId ->  AttributeSet(attr)).toMap
      parent.map { attr =>
          childMap.getOrElse(attr.exprId, AttributeSet.empty)
      }.reduce(mergeColumns) ++ child
    }
  }

  private def getExpressionSubqueryPlans(expression: Expression): Seq[LogicalPlan] = {
    expression match {
      case s: ScalarSubquery => Seq(s.plan)
      case s => s.children.flatMap(getExpressionSubqueryPlans)
    }
  }

  private def findSparkPlanLogicalLink(sparkPlans: Seq[SparkPlan]): Option[LogicalPlan] = {
    sparkPlans.find(_.logicalLink.nonEmpty) match {
      case Some(sparkPlan) => sparkPlan.logicalLink
      case None => findSparkPlanLogicalLink(sparkPlans.flatMap(_.children))
    }
  }

  private def containsCountAll(expr: Expression): Boolean = {
    expr match {
      case e: Count if e.references.isEmpty => true
      case e =>
        e.children.exists(containsCountAll)
    }
  }

  private def getSelectColumns(
                                      named: Seq[NamedExpression],
                                      inputTablesByPlan: mutable.HashSet[String]): AttributeSet = {
    val exps = named.map {
      case exp: Alias =>
        val references =
          if (exp.references.nonEmpty) exp.references
          else {
            val attrRefs = getExpressionSubqueryPlans(exp.child)
              .map(extractColumns(_, AttributeSet.empty, inputTablesByPlan))
              .foldLeft(AttributeSet.empty)(mergeColumns)
              .map(attr => attr.withQualifier(attr.qualifier :+ SUBQUERY_COLUMN_IDENTIFIER))
            AttributeSet(attrRefs)
          }
        if (!containsCountAll(exp.child)) references
        else references + exp.toAttribute.withName(AGGREGATE_COUNT_COLUMN_IDENTIFIER)
      case a: Attribute => AttributeSet(a)
    }

    exps.reduce(mergeColumns)
  }

  private def joinRelationColumns(
                                         parent: AttributeSet,
                                         relationAttrs: Seq[Attribute],
                                         qualifier: Seq[String]): AttributeSet = {
    val relationAttrSet = AttributeSet(relationAttrs)
    if (parent.nonEmpty) {
      AttributeSet(parent.collect {
        case attr if relationAttrSet.contains(attr) =>
          attr.withQualifier(qualifier)
        case attr
          if attr.qualifier.nonEmpty &&
            attr.qualifier.last.equalsIgnoreCase(SUBQUERY_COLUMN_IDENTIFIER) =>
          attr.withQualifier(attr.qualifier.init)
//        case attr if attr.name.equalsIgnoreCase(AGGREGATE_COUNT_COLUMN_IDENTIFIER) =>
//          attr.withQualifier(qualifier)
        case attr if isNameWithQualifier(attr, qualifier) =>
          val newName = attr.name.split('.').last.stripPrefix("`").stripSuffix("`")
          attr.withName(newName).withQualifier(qualifier)
      })
    } else {
      relationAttrs.map { attr => AttributeSet(attr.withQualifier(qualifier))}.reduce(mergeColumns)
    }
  }

  private def isNameWithQualifier(attr: Attribute, qualifier: Seq[String]): Boolean = {
    val nameTokens = attr.name.split('.')
    val namespace = nameTokens.init.mkString(".")
    nameTokens.length > 1 && namespace.endsWith(qualifier.mkString("."))
  }

  private def extractColumns(plan: LogicalPlan,
          parentColumns: AttributeSet,
          inputTablesByPlan: mutable.HashSet[String]): AttributeSet = {

    plan match {
      // For command
      case p if p.nodeName == "CommandResult" =>
        val commandPlan = getField[LogicalPlan](plan, "commandLogicalPlan")
        extractColumns(commandPlan, parentColumns, inputTablesByPlan)

      case p if p.nodeName == "AlterViewAsCommand" =>
        val query =
          if (SPARK_RUNTIME_VERSION <= "3.1") {
            sparkSession.sessionState.analyzer.execute(getQuery(plan))
          } else {
            getQuery(plan)
          }
        extractColumns(query, parentColumns, inputTablesByPlan)

      case p
        if p.nodeName == "CreateViewCommand"
          && getField[ViewType](plan, "viewType") == PersistedView =>

        val query =
          if (SPARK_RUNTIME_VERSION <= "3.1") {
            sparkSession.sessionState.analyzer.execute(getField[LogicalPlan](plan, "child"))
          } else {
            getField[LogicalPlan](plan, "plan")
          }

        extractColumns(query, parentColumns, inputTablesByPlan)

      case p
        if p.nodeName == "CreateDataSourceTableAsSelectCommand" ||
          p.nodeName == "CreateHiveTableAsSelectCommand" ||
          p.nodeName == "OptimizedCreateHiveTableAsSelectCommand" ||
          p.nodeName == "CreateTableAsSelect" ||
          p.nodeName == "ReplaceTableAsSelect" ||
          p.nodeName == "InsertIntoDataSourceCommand" ||
          p.nodeName == "InsertIntoHadoopFsRelationCommand" ||
          p.nodeName == "InsertIntoDataSourceDirCommand" ||
          p.nodeName == "InsertIntoHiveDirCommand" ||
          p.nodeName == "InsertIntoHiveTable" ||
          p.nodeName == "SaveIntoDataSourceCommand" ||

          p.nodeName == "AppendData" ||
          p.nodeName == "OverwriteByExpression" ||
          p.nodeName == "OverwritePartitionsDynamic" ||

          p.nodeName == "WriteDelta" ||
          p.nodeName == "ReplaceData" =>

        extractColumns(getQuery(plan), parentColumns, inputTablesByPlan)

      case p if p.nodeName == "MergeRows" =>
        p.children.map(extractColumns(_, parentColumns, inputTablesByPlan))
          .reduce(mergeColumns)

      case p if p.nodeName == "MergeIntoTable" =>
        val matchedActions = getField[Seq[MergeAction]](plan, "matchedActions")
        val notMatchedActions = getField[Seq[MergeAction]](plan, "notMatchedActions")
        val allAssignments = (matchedActions ++ notMatchedActions).collect {
          case ua: UpdateAction => ua.assignments
          case ia: InsertAction => ia.assignments
        }.flatten
        val nextColumns = allAssignments
          .map(assignment => assignment.key.references ++ assignment.value.references)
          .reduce(mergeColumns)

        val targetTable = getField[LogicalPlan](plan, "targetTable")
        val sourceTable = getField[LogicalPlan](plan, "sourceTable")

        val targetColumns = extractColumns(
          targetTable,
          nextColumns,
          inputTablesByPlan)
        val sourceColumns = extractColumns(
          sourceTable,
          nextColumns,
          inputTablesByPlan)

        targetColumns ++ sourceColumns

      case p if p.nodeName == "WithCTE" =>
        val optimized = sparkSession.sessionState.optimizer.execute(p)
        extractColumns(optimized, parentColumns, inputTablesByPlan)

      // For query
      case p: Project =>
        val nextColumns = joinColumns(parentColumns,
          getSelectColumns(p.projectList, inputTablesByPlan))

        p.children.map(extractColumns(_, nextColumns, inputTablesByPlan))
          .reduce(mergeColumns)

      case p: Aggregate =>
        val nextColumns = joinColumns(parentColumns,
            getSelectColumns(p.aggregateExpressions, inputTablesByPlan))

        p.children.map(extractColumns(_, nextColumns, inputTablesByPlan))
          .reduce(mergeColumns)

      case p: Expand =>
        val references =
          p.projections.transpose.map(_.flatMap(x => x.references))
            .map(AttributeSet(_)).reduce(mergeColumns)

        val childColumns = references
        val nextColumns = joinColumns(parentColumns, childColumns)
        p.children.map(extractColumns(_, nextColumns, inputTablesByPlan))
          .reduce(mergeColumns)

      case p: Generate =>
        val nextColumns = parentColumns ++ p.references

        p.children.map(extractColumns(
          _,
          nextColumns,
          inputTablesByPlan)).reduce(mergeColumns)

      case p: Window =>
        val windowColumns = p.windowExpressions.map(exp => exp.references).reduce(mergeColumns)

        val nextColumns = if (parentColumns.isEmpty) {
          p.child.output.map(attr => attr.references).reduce(mergeColumns) ++ windowColumns
        } else {
          parentColumns ++ windowColumns
        }

        p.children.map(extractColumns(
          _,
          nextColumns,
          inputTablesByPlan)).reduce(mergeColumns)

      case p: Join =>
        val nextColumns = if (p.condition.isEmpty) {
          parentColumns
        } else {
          val conditionColumns = p.condition.map(exp => exp.references).reduce(mergeColumns)
          parentColumns ++ conditionColumns
        }

        p.joinType match {
          case LeftSemi | LeftAnti =>
            extractColumns(p.right, nextColumns, inputTablesByPlan) ++
            extractColumns(p.left, nextColumns, inputTablesByPlan)
          case _ =>
            p.children.map(extractColumns(_, nextColumns, inputTablesByPlan))
              .reduce(mergeColumns)
        }

      case p: Union =>
        val childrenColumns = p.children.map(
            extractColumns(_, AttributeSet.empty, inputTablesByPlan))
          .reduce(mergeColumns)

        joinColumns(parentColumns, childrenColumns)

      case p: LogicalRelation if p.catalogTable.nonEmpty =>
        val tableName = getV1TableName(p.catalogTable.get.qualifiedName)
        inputTablesByPlan += tableName
        joinRelationColumns(parentColumns, p.output, Seq(tableName))

      case p: HiveTableRelation =>
        val tableName = getV1TableName(p.tableMeta.qualifiedName)
        inputTablesByPlan += tableName
        joinRelationColumns(parentColumns, p.output, Seq(tableName))

      case p: DataSourceV2ScanRelation =>
        val tableName = getV2TableName(p)
        inputTablesByPlan += tableName
        joinRelationColumns(parentColumns, p.output, Seq(tableName))

      // For creating the view from v2 table, the logical plan of table will
      // be the `DataSourceV2Relation` not the `DataSourceV2ScanRelation`.
      // because the view from the table is not going to read it.
      case p: DataSourceV2Relation =>
        val tableName = getV2TableName(p)
        inputTablesByPlan += tableName
        joinRelationColumns(parentColumns, p.output, Seq(tableName))

      case p: LocalRelation =>
        inputTablesByPlan += LOCAL_TABLE_IDENTIFIER
        joinRelationColumns(parentColumns, p.output, Seq(LOCAL_TABLE_IDENTIFIER))

      case _: OneRowRelation =>
        parentColumns.map {
          case attr
            if attr.qualifier.nonEmpty && attr.qualifier.last.equalsIgnoreCase(
              SUBQUERY_COLUMN_IDENTIFIER) =>
            attr.withQualifier(attr.qualifier.init)
          case attr => attr
        }.map(AttributeSet(_)).reduce(mergeColumns)
      // PermanentViewMarker is introduced by kyuubi authz plugin, which is a wrapper of View,
      // so we just extract the columns lineage from its inner children (original view)
      case pvm if pvm.nodeName == "PermanentViewMarker" =>
        pvm.innerChildren.asInstanceOf[Seq[LogicalPlan]]
          .map(extractColumns(_, parentColumns, inputTablesByPlan))
          .reduce(mergeColumns)

      case p: View =>
        if (!p.isTempView && SparkContextHelper.getConf(
          LineageConf.SKIP_PARSING_PERMANENT_VIEW_ENABLED)) {
          val viewName = getV1TableName(p.desc.qualifiedName)
          inputTablesByPlan += viewName
          joinRelationColumns(parentColumns, p.output, Seq(viewName))
        } else {
          val viewColumnsLineage =
            extractColumns(p.child, AttributeSet.empty, inputTablesByPlan)
          joinColumns(parentColumns, viewColumnsLineage)
        }

      case p: InMemoryRelation =>
        // get logical plan from cachedPlan
        val cachedTableLogical = findSparkPlanLogicalLink(Seq(p.cacheBuilder.cachedPlan))
        cachedTableLogical match {
          case Some(logicPlan) =>
            val relationColumnLineage =
              extractColumns(
                logicPlan,
                AttributeSet.empty,
                inputTablesByPlan)
            joinColumns(parentColumns, relationColumnLineage)
          case _ =>
            joinRelationColumns(
              parentColumns,
              p.output,
              p.cacheBuilder.tableName.toSeq)
        }

      case p: Filter =>
        val filterColumns = p.condition.map(exp => exp.references).reduce(mergeColumns)
        val nextColumns = parentColumns ++ filterColumns

        p.children.map(extractColumns(
          _,
          nextColumns,
          inputTablesByPlan)).reduce(mergeColumns)

      case p if p.children.isEmpty => AttributeSet.empty

      case p =>
        p.children.map(extractColumns(
          _,
          parentColumns,
          inputTablesByPlan)).reduce(mergeColumns)
    }
  }

  private def getQuery(plan: LogicalPlan): LogicalPlan = getField[LogicalPlan](plan, "query")

  private def getV2TableName(plan: NamedRelation): String = {
    plan match {
      case relation: DataSourceV2ScanRelation =>
        val catalog = relation.relation.catalog.map(_.name()).getOrElse(LineageConf.DEFAULT_CATALOG)
        val database = relation.relation.identifier.get.namespace().mkString(".")
        val table = relation.relation.identifier.get.name()
        s"$catalog.$database.$table"
      case relation: DataSourceV2Relation =>
        val catalog = relation.catalog.map(_.name()).getOrElse(LineageConf.DEFAULT_CATALOG)
        val database = relation.identifier.get.namespace().mkString(".")
        val table = relation.identifier.get.name()
        s"$catalog.$database.$table"
      case _ =>
        plan.name
    }
  }

  private def getV1TableName(qualifiedName: String): String = {
    qualifiedName.split("\\.") match {
      case Array(database, table) =>
        Seq(LineageConf.DEFAULT_CATALOG, database, table).filter(_.nonEmpty).mkString(".")
      case _ => qualifiedName
    }
  }

}

case class SparkSQLColumnParseHelper(sparkSession: SparkSession) extends ColumnParser
  with Logging {

  def extractColumn(
                          executionId: Long,
                          plan: LogicalPlan): Option[List[String]] = {
    Try(parse(plan)).recover {
      case e: Exception =>
        logWarning(s"Extract Statement[$executionId] columns failed.", e)
        throw e
    }.toOption
  }

}